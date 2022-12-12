package dump

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/liweiyi88/onedump/driver"
	"github.com/liweiyi88/onedump/storage"
	"golang.org/x/crypto/ssh"
)

type Dump struct {
	Jobs []*Job `yaml:"jobs"`
}

func (dump *Dump) Validate() error {
	errorCollection := make([]string, 0)

	for _, job := range dump.Jobs {
		err := job.validate()
		if err != nil {
			errorCollection = append(errorCollection, err.Error())
		}
	}

	if len(errorCollection) == 0 {
		return nil
	}

	return errors.New(strings.Join(errorCollection, ","))
}

type JobResult struct {
	Error   error
	JobName string
	Elapsed time.Duration
}

func (result *JobResult) Print() {
	if result.Error != nil {
		fmt.Printf("Job: %s failed, it took %s with error: %v \n", result.JobName, result.Elapsed, result.Error)
	} else {
		fmt.Printf("Job: %s succeeded, it took %v \n", result.JobName, result.Elapsed)
	}
}

type Job struct {
	DumpFile       string                  `yaml:"dumpfile"`
	Name           string                  `yaml:"name"`
	DBDriver       string                  `yaml:"dbdriver"`
	DBDsn          string                  `yaml:"dbdsn"`
	Gzip           bool                    `yaml:"gzip"`
	SshHost        string                  `yaml:"sshhost"`
	SshUser        string                  `yaml:"sshuser"`
	PrivateKeyFile string                  `yaml:"privatekeyfile"`
	DumpOptions    []string                `yaml:"options"`
	S3             *storage.AWSCredentials `yaml:"s3"`
}

type Option func(job *Job)

func WithSshHost(sshHost string) Option {
	return func(job *Job) {
		job.SshHost = sshHost
	}
}

func WithSshUser(sshUser string) Option {
	return func(job *Job) {
		job.SshUser = sshUser
	}
}

func WithGzip(gzip bool) Option {
	return func(job *Job) {
		job.Gzip = gzip
	}
}

func WithDumpOptions(dumpOptions []string) Option {
	return func(job *Job) {
		job.DumpOptions = dumpOptions
	}
}

func WithPrivateKeyFile(privateKeyFile string) Option {
	return func(job *Job) {
		job.PrivateKeyFile = privateKeyFile
	}
}

func NewJob(name, driver, dumpFile, dbDsn string, opts ...Option) *Job {
	job := &Job{
		Name:     name,
		DBDriver: driver,
		DumpFile: dumpFile,
		DBDsn:    dbDsn,
	}

	for _, opt := range opts {
		opt(job)
	}

	return job
}

func (job Job) validate() error {
	if strings.TrimSpace(job.Name) == "" {
		return errors.New("job name is required")
	}

	if strings.TrimSpace(job.DumpFile) == "" {
		return errors.New("dump file path is required")
	}

	if strings.TrimSpace(job.DBDsn) == "" {
		return errors.New("databse dsn is required")
	}

	if strings.TrimSpace(job.DBDriver) == "" {
		return errors.New("databse driver is required")
	}

	return nil
}

func (job *Job) viaSsh() bool {
	if strings.TrimSpace(job.SshHost) != "" && strings.TrimSpace(job.SshUser) != "" && strings.TrimSpace(job.PrivateKeyFile) != "" {
		return true
	}

	return false
}

func (job *Job) getDBDriver() (driver.Driver, error) {
	switch job.DBDriver {
	case "mysql":
		driver, err := driver.NewMysqlDriver(job.DBDsn, job.DumpOptions, job.viaSsh())
		if err != nil {
			return nil, err
		}

		return driver, nil
	default:
		return nil, fmt.Errorf("%s is not a supported database driver", job.DBDriver)
	}
}

func ensureHaveSSHPort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "22")
	}
	return addr
}

func (job *Job) sshDump() error {
	driver, err := job.getDBDriver()
	if err != nil {
		return fmt.Errorf("job %s, failed to create db driver %v", job.Name, err)
	}

	host := ensureHaveSSHPort(job.SshHost)

	pKey, err := os.ReadFile(job.PrivateKeyFile)
	if err != nil {
		return fmt.Errorf("can not read the private key file :%w", err)
	}

	signer, err := ssh.ParsePrivateKey(pKey)
	if err != nil {
		return fmt.Errorf("failed to create singer :%w", err)
	}

	conf := &ssh.ClientConfig{
		User:            job.SshUser,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
	}

	client, err := ssh.Dial("tcp", host, conf)
	if err != nil {
		return fmt.Errorf("failed to dial remote server via ssh: %w", err)
	}

	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("failed to start ssh session: %w", err)
	}

	defer session.Close()

	err = job.dump(session, driver)
	if err != nil {
		return err
	}

	return nil
}

func (job *Job) execDump() error {
	driver, err := job.getDBDriver()
	if err != nil {
		return fmt.Errorf("job %s, failed to crete db driver: %v", job.Name, err)
	}

	command, args, err := driver.GetDumpCommand()
	if err != nil {
		return fmt.Errorf("job %s failed to get dump command: %v", job.Name, err)
	}

	cmd := exec.Command(command, args...)

	job.dump(cmd, driver)
	if err != nil {
		return err
	}

	return nil
}

func (job *Job) Run() *JobResult {
	start := time.Now()
	var result JobResult

	defer func() {
		elapsed := time.Since(start)
		result.Elapsed = elapsed
	}()

	result.JobName = job.Name

	if job.viaSsh() {
		err := job.sshDump()
		if err != nil {
			result.Error = fmt.Errorf("job %s, failed to run ssh dump command: %v", job.Name, err)
		}

		return &result
	}

	err := job.execDump()
	if err != nil {
		result.Error = fmt.Errorf("job %s, failed to run dump command: %v", job.Name, err)

	}

	return &result
}

func (job *Job) dumpToFile(runner any, driver driver.Driver, store storage.Storage) error {
	file, err := store.CreateDumpFile()
	if err != nil {
		return fmt.Errorf("failed to create storage dump file: %w", err)
	}

	var gzipWriter *gzip.Writer
	if job.Gzip {
		gzipWriter = gzip.NewWriter(file)
	}

	defer func() {
		if gzipWriter != nil {
			gzipWriter.Close()
		}

		file.Close()
	}()

	switch runner := runner.(type) {
	case *exec.Cmd:
		runner.Stderr = os.Stderr
		if gzipWriter != nil {
			runner.Stdout = gzipWriter
		} else {
			runner.Stdout = file
		}

		if err := runner.Run(); err != nil {
			return fmt.Errorf("remote command error: %v", err)
		}
	case *ssh.Session:
		var remoteErr bytes.Buffer
		runner.Stderr = &remoteErr
		if gzipWriter != nil {
			runner.Stdout = gzipWriter
		} else {
			runner.Stdout = file
		}

		sshCommand, err := driver.GetSshDumpCommand()
		if err != nil {
			return fmt.Errorf("failed to get ssh dump command %w", err)
		}

		if err := runner.Run(sshCommand); err != nil {
			return fmt.Errorf("remote command error: %s, %v", remoteErr.String(), err)
		}
	default:
		return errors.New("unsupport runner type")
	}

	return nil
}

// The core function that dump db content to a file (locally or remotely).
// It checks the filename to determine if we need to upload the file to remote storage or keep it locally.
// For uploading file to S3 bucket, the filename shold follow the pattern: s3://<bucket_name>/<key> .
// For any remote upload, we try to cache it in a local dir then upload it to the remote storage.
func (job *Job) dump(runner any, driver driver.Driver) error {
	store, err := job.createStorage()
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	err = job.dumpToFile(runner, driver, store)
	if err != nil {
		return err
	}

	cloudStore, ok := store.(storage.CloudStorage)

	if ok {
		err := cloudStore.Upload()
		if err != nil {
			return fmt.Errorf("failed to upload file to cloud storage: %w", err)
		}
	}

	return nil
}

// Factory method to create the storage struct based on filename.
func (job *Job) createStorage() (storage.Storage, error) {
	filename := ensureFileSuffix(job.DumpFile, job.Gzip)
	s3Storage, ok, err := storage.CreateS3Storage(filename, job.S3)

	if err != nil {
		return nil, err
	}

	if ok {
		return s3Storage, nil
	}

	return &storage.LocalStorage{
		Filename: filename,
	}, nil
}

// Ensure a file has proper file extension.
func ensureFileSuffix(filename string, shouldGzip bool) string {
	if !shouldGzip {
		return filename
	}

	if strings.HasSuffix(filename, ".gz") {
		return filename
	}

	return filename + ".gz"
}

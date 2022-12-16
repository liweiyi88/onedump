package dump

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
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

	err = job.dump(session)
	if err != nil {
		return err
	}

	return nil
}

func (job *Job) execDump() error {
	err := job.dump(nil)
	if err != nil {
		return fmt.Errorf("failed to exec command dump: %w", err)
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

func (job *Job) writeToFile(sshSession *ssh.Session, file io.Writer) error {
	var gzipWriter *gzip.Writer
	if job.Gzip {
		gzipWriter = gzip.NewWriter(file)
		defer gzipWriter.Close()
	}

	driver, err := job.getDBDriver()
	if err != nil {
		return fmt.Errorf("failed to get db driver: %w", err)
	}

	if sshSession != nil {
		var remoteErr bytes.Buffer
		sshSession.Stderr = &remoteErr
		if gzipWriter != nil {
			sshSession.Stdout = gzipWriter
		} else {
			sshSession.Stdout = file
		}

		sshCommand, err := driver.GetSshDumpCommand()
		if err != nil {
			return fmt.Errorf("failed to get ssh dump command %w", err)
		}

		if err := sshSession.Run(sshCommand); err != nil {
			return fmt.Errorf("remote command error: %s, %v", remoteErr.String(), err)
		}

		return nil
	}

	command, args, err := driver.GetDumpCommand()
	if err != nil {
		return fmt.Errorf("job %s failed to get dump command: %v", job.Name, err)
	}

	cmd := exec.Command(command, args...)

	cmd.Stderr = os.Stderr
	if gzipWriter != nil {
		cmd.Stdout = gzipWriter
	} else {
		cmd.Stdout = file
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("remote command error: %v", err)
	}

	return nil
}

func (job *Job) dumpToFile(filename string, sshSession *ssh.Session) (string, error) {
	dumpFileName := ensureFileSuffix(filename, job.Gzip)

	file, err := os.Create(dumpFileName)
	if err != nil {
		return "", fmt.Errorf("failed to create dump file: %w", err)
	}

	defer file.Close()

	err = job.writeToFile(sshSession, file)
	if err != nil {
		return "", fmt.Errorf("failed to write dump content to file: %w,", err)
	}

	return file.Name(), nil
}

// The core function that dump db content to a file (locally or remotely).
// It checks the filename to determine if we need to upload the file to remote storage or keep it locally.
// For uploading file to S3 bucket, the filename shold follow the pattern: s3://<bucket_name>/<key> .
// For any remote upload, we try to cache it in a local dir then upload it to the remote storage.
func (job *Job) dump(sshSession *ssh.Session) error {
	filename := job.DumpFile

	store := job.createCloudStorage(filename)
	if store != nil {
		err := os.MkdirAll(storage.UploadCacheDir(), 0750)
		if err != nil {
			return fmt.Errorf("failed to create upload cache dir for remote upload. %w", err)
		}

		defer func() {
			err = os.RemoveAll(storage.UploadCacheDir())
			if err != nil {
				log.Println("failed to remove cache dir after dump", err)
			}
		}()

		filename = storage.UploadCacheFilePath()
	}

	dumpFile, err := job.dumpToFile(filename, sshSession)

	if err != nil {
		return fmt.Errorf("failed to dump db content to file %w: ", err)
	}

	if store != nil {
		uploadFile, err := os.Open(dumpFile)
		if err != nil {
			return fmt.Errorf("failed to open the cached dump file %w", err)
		}

		defer uploadFile.Close()

		err = store.Upload(uploadFile)
		if err != nil {
			return fmt.Errorf("failed to upload file to cloud storage: %w", err)
		}

		log.Printf("successfully upload dump file to %s", store.CloudFilePath())
	}

	return nil
}

// Factory method to create the cloud storage struct based on filename.
func (job *Job) createCloudStorage(filename string) storage.CloudStorage {
	name := strings.TrimSpace(filename)

	if strings.HasPrefix(name, storage.S3Prefix) {
		path := strings.TrimPrefix(name, storage.S3Prefix)
		pathChunks := strings.Split(path, "/")

		if len(pathChunks) < 2 {
			panic(storage.ErrInvalidS3Path)
		}

		bucket := pathChunks[0]
		key := strings.Join(pathChunks[1:], "/")

		return storage.NewS3Storage(bucket, key, job.S3)
	}

	return nil
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

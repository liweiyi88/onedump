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
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/liweiyi88/onedump/driver"
	"github.com/liweiyi88/onedump/storage"
	"github.com/liweiyi88/onedump/storage/gdrive"
	"github.com/liweiyi88/onedump/storage/local"
	"github.com/liweiyi88/onedump/storage/s3"
	"golang.org/x/crypto/ssh"
)

var (
	ErrMissingJobName  = errors.New("job name is required")
	ErrMissingDBDsn    = errors.New("databse dsn is required")
	ErrMissingDBDriver = errors.New("databse driver is required")
)

type Dump struct {
	Jobs []*Job `yaml:"jobs"`
}

func (dump *Dump) Validate() error {
	var errs error

	for _, job := range dump.Jobs {
		err := job.validate()
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}

	return errs
}

type JobResult struct {
	Error   error
	JobName string
	Elapsed time.Duration
}

func (result *JobResult) String() string {
	if result.Error != nil {
		return fmt.Sprintf("Job: %s failed, it took %s with error: %v", result.JobName, result.Elapsed, result.Error)
	}

	return fmt.Sprintf("Job: %s succeeded, it took %v", result.JobName, result.Elapsed)
}

type Job struct {
	Name        string   `yaml:"name"`
	DBDriver    string   `yaml:"dbdriver"`
	DBDsn       string   `yaml:"dbdsn"`
	Gzip        bool     `yaml:"gzip"`
	Unique      bool     `yaml:"unique"`
	SshHost     string   `yaml:"sshhost"`
	SshUser     string   `yaml:"sshuser"`
	SshKey      string   `yaml:"sshkey"`
	DumpOptions []string `yaml:"options"`
	Storage     struct {
		Local  []*local.Local   `yaml:"local"`
		S3     []*s3.S3         `yaml:"s3"`
		GDrive []*gdrive.GDrive `yaml:"gdrive"`
	} `yaml:"storage"`
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

func WithDumpOptions(dumpOptions ...string) Option {
	return func(job *Job) {
		job.DumpOptions = dumpOptions
	}
}

func WithSshKey(sshKey string) Option {
	return func(job *Job) {
		job.SshKey = sshKey
	}
}

func NewJob(name, driver, dbDsn string, opts ...Option) *Job {
	job := &Job{
		Name:     name,
		DBDriver: driver,
		DBDsn:    dbDsn,
	}

	for _, opt := range opts {
		opt(job)
	}

	return job
}

func (job Job) validate() error {
	if strings.TrimSpace(job.Name) == "" {
		return ErrMissingJobName
	}

	if strings.TrimSpace(job.DBDsn) == "" {
		return ErrMissingDBDsn
	}

	if strings.TrimSpace(job.DBDriver) == "" {
		return ErrMissingDBDriver
	}

	return nil
}

func (job *Job) viaSsh() bool {
	if strings.TrimSpace(job.SshHost) != "" && strings.TrimSpace(job.SshUser) != "" && strings.TrimSpace(job.SshKey) != "" {
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

	signer, err := ssh.ParsePrivateKey([]byte(job.SshKey))
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

	defer func() {
		// Do not need to call session.Close() here as it will only give EOF error.
		err = client.Close()
		if err != nil {
			log.Printf("failed to close ssh client: %v", err)
		}
	}()

	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("failed to start ssh session: %w", err)
	}

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

func (job *Job) dumpToFile(sshSession *ssh.Session, file io.Writer) error {
	var gzipWriter *gzip.Writer
	if job.Gzip {
		gzipWriter = gzip.NewWriter(file)
		defer func() {
			err := gzipWriter.Close()
			if err != nil {
				log.Printf("failed to close gzip writer: %v", err)
			}
		}()
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
		return fmt.Errorf("command error: %v", err)
	}

	return nil
}

func (job *Job) dumpToCacheFile(sshSession *ssh.Session) (string, func(), error) {
	file, cacheDir, err := storage.CreateCacheFile(job.Gzip)

	cleanup := func() {
		err := os.RemoveAll(cacheDir)
		if err != nil {
			log.Println("failed to remove cache dir after dump", err)
		}
	}

	if err != nil {
		return "", cleanup, err
	}

	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close cache file: %v", err)
		}
	}()

	err = job.dumpToFile(sshSession, file)
	if err != nil {
		return "", cleanup, fmt.Errorf("failed to dump content to file: %w,", err)
	}

	// We have to close the file in defer function and returns filename instead of returing the fd (os.File)
	// Otherwise if we pass the fd and the storage func reuse the same fd, the file will be corrupted.
	return file.Name(), cleanup, nil
}

// The core function that dump db content to a file (locally or remotely).
// It checks the filename to determine if we need to upload the file to remote storage or keep it locally.
// For uploading file to S3 bucket, the filename shold follow the pattern: s3://<bucket_name>/<key> .
// For any remote upload, we try to cache it in a local dir then upload it to the remote storage.
func (job *Job) dump(sshSession *ssh.Session) error {
	cacheFileName, cleanup, err := job.dumpToCacheFile(sshSession)
	defer cleanup()

	if err != nil {
		return fmt.Errorf("failed to dump content to cache file: %v", err)
	}

	cacheFile, err := os.Open(cacheFileName)
	if err != nil {
		return fmt.Errorf("failed to open cache file: %v", err)
	}

	defer func() {
		err := cacheFile.Close()
		if err != nil {
			log.Printf("failed to close cache file: %v", err)
		}
	}()

	err = job.store(cacheFile)
	if err != nil {
		return fmt.Errorf("failed to store dump file %v", err)
	}

	return nil
}

// Save dump file to desired storages.
func (job *Job) store(cacheFile io.Reader) error {
	storages := job.getStorages()
	numberOfStorages := len(storages)

	var err error

	if numberOfStorages > 0 {
		// Use pipe to pass content from the cache file to different writer.
		readers, writer, closer := storageReadWriteCloser(numberOfStorages)

		go func() {
			_, e := io.Copy(writer, cacheFile)
			if e != nil {
				multierror.Append(err, e)
			}
			closer.Close()
		}()

		var wg sync.WaitGroup
		wg.Add(numberOfStorages)
		for i, s := range storages {
			storage := s
			go func(i int) {
				defer wg.Done()
				e := storage.Save(readers[i], job.Gzip, job.Unique)
				if e != nil {
					err = multierror.Append(err, e)
				}
			}(i)
		}

		wg.Wait()
	}

	return err
}

func (job *Job) getStorages() []storage.Storage {
	var storages []storage.Storage
	if len(job.Storage.Local) > 0 {
		for _, v := range job.Storage.Local {
			storages = append(storages, v)
		}
	}

	if len(job.Storage.S3) > 0 {
		for _, v := range job.Storage.S3 {
			storages = append(storages, v)
		}
	}

	if len(job.Storage.GDrive) > 0 {
		for _, v := range job.Storage.GDrive {
			storages = append(storages, v)
		}
	}

	return storages
}

// Pipe readers, writers and closer for fanout the same os.file
func storageReadWriteCloser(count int) ([]io.Reader, io.Writer, io.Closer) {
	var prs []io.Reader
	var pws []io.Writer
	var pcs []io.Closer
	for i := 0; i < count; i++ {
		pr, pw := io.Pipe()

		prs = append(prs, pr)
		pws = append(pws, pw)
		pcs = append(pcs, pw)
	}

	return prs, io.MultiWriter(pws...), NewMultiCloser(pcs)
}

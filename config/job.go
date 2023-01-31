package config

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/liweiyi88/onedump/driver"
	"github.com/liweiyi88/onedump/storage/dropbox"
	"github.com/liweiyi88/onedump/storage/gdrive"
	"github.com/liweiyi88/onedump/storage/local"
	"github.com/liweiyi88/onedump/storage/s3"
)

var (
	ErrMissingJobName  = errors.New("job name is required")
	ErrMissingDBDsn    = errors.New("databse dsn is required")
	ErrMissingDBDriver = errors.New("databse driver is required")
)

type Storage interface {
	Save(reader io.Reader, gzip bool, unique bool) error
}

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
		Local   []*local.Local     `yaml:"local"`
		S3      []*s3.S3           `yaml:"s3"`
		GDrive  []*gdrive.GDrive   `yaml:"gdrive"`
		Dropbox []*dropbox.Dropbox `yaml:"dropbox"`
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

func (job *Job) ViaSsh() bool {
	if strings.TrimSpace(job.SshHost) != "" && strings.TrimSpace(job.SshUser) != "" && strings.TrimSpace(job.SshKey) != "" {
		return true
	}

	return false
}

func (job *Job) GetDBDriver() (driver.Driver, error) {
	switch job.DBDriver {
	case "mysql":
		driver, err := driver.NewMysqlDriver(job.DBDsn, job.DumpOptions, job.ViaSsh())
		if err != nil {
			return nil, err
		}

		return driver, nil
	default:
		return nil, fmt.Errorf("%s is not a supported database driver", job.DBDriver)
	}
}

func (job *Job) GetStorages() []Storage {
	var storages []Storage
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

	if len(job.Storage.Dropbox) > 0 {
		for _, v := range job.Storage.Dropbox {
			storages = append(storages, v)
		}
	}

	return storages
}

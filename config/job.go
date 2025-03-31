package config

import (
	"errors"
	"strings"

	"github.com/liweiyi88/onedump/notifier/slack"
	"github.com/liweiyi88/onedump/storage/dropbox"
	"github.com/liweiyi88/onedump/storage/gdrive"
	"github.com/liweiyi88/onedump/storage/local"
	"github.com/liweiyi88/onedump/storage/s3"
	"github.com/liweiyi88/onedump/storage/sftp"
)

var (
	ErrMissingJobName  = errors.New("job name is required")
	ErrMissingDBDsn    = errors.New("databse dsn is required")
	ErrMissingDBDriver = errors.New("databse driver is required")
)

type Dump struct {
	Notifier struct {
		Slack []*slack.Slack `yaml:"slack"`
	} `yaml:"notifier"`
	Jobs []*Job `yaml:"jobs"`
}

func (dump *Dump) Validate() error {
	var errs error

	for _, job := range dump.Jobs {
		err := job.validate()
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}

	return errs
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
		Sftp    []*sftp.Sftp       `yaml:"sftp"`
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

package dumpjob

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liweiyi88/onedump/dump"
)

type OneDump struct {
	Jobs []Job `yaml:"jobs"`
}

func (oneDump *OneDump) Validate() error {
	errorCollection := make([]string, 0)

	for _, job := range oneDump.Jobs {
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

type Job struct {
	DumpFile       string   `yaml:"dumpfile"`
	Name           string   `yaml:"name"`
	DBDriver       string   `yaml:"dbdriver"`
	DBDsn          string   `yaml:"dbdsn"`
	Gzip           bool     `yaml:"gzip"`
	SshHost        string   `yaml:"sshhost"`
	SshUser        string   `yaml:"sshuser"`
	PrivateKeyFile string   `yaml:"privatekeyfile"`
	Options        []string `yaml:"options"`
}

func (job Job) validate() error {
	if job.Name == "" {
		return errors.New("job name is required")
	}

	if job.DumpFile == "" {
		return errors.New("dump file path is required")
	}

	if job.DBDsn == "" {
		return errors.New("databse dsn is required")
	}

	if job.DBDriver == "" {
		return errors.New("databse driver is required")
	}

	return nil
}

func (job Job) viaSsh() bool {
	if job.SshHost != "" && job.SshUser != "" && job.PrivateKeyFile != "" {
		return true
	}

	return false
}

func (job Job) Run() *JobResult {
	start := time.Now()
	var result JobResult

	defer func() {
		elapsed := time.Since(start)
		result.Elapsed = elapsed
	}()

	result.JobName = job.Name

	if job.viaSsh() {
		command, err := dump.GetDumpCommand(job.DBDriver, job.DBDsn, job.DumpFile, job.Options)
		if err != nil {
			result.Error = fmt.Errorf("job %s, failed to get dump command: %v", job.Name, err)
			return &result
		}

		sshDumper := dump.NewSshDumper(job.SshHost, job.SshUser, job.PrivateKeyFile)
		err = sshDumper.Dump(job.DumpFile, command, job.Gzip)
		if err != nil {
			result.Error = fmt.Errorf("job %s, failed to run dump command: %v", job.Name, err)
		}
	} else {
		dumper, err := dump.NewMysqlDumper(job.DBDsn, job.Options, false)
		if err != nil {
			result.Error = fmt.Errorf("job %s, failed to crete  mysql dumper: %v", job.Name, err)
			return &result
		}

		err = dumper.Dump(job.DumpFile, job.Gzip)
		if err != nil {
			result.Error = fmt.Errorf("job %s, failed to dump mysql dumper: %v", job.Name, err)
			return &result
		}
	}

	return &result
}

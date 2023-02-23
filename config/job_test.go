package config

import (
	"errors"
	"testing"
	"time"

	"github.com/liweiyi88/onedump/dumper"
)

var testDBDsn = "root@tcp(127.0.0.1:3306)/dump_test"
var testPsqlDBDsn = "postgres://julianli:julian@localhost:5432/mypsqldb"

func TestGetDBDriver(t *testing.T) {
	job := NewJob("job1", "mysql", testDBDsn)

	_, err := job.getDBDriver()
	if err != nil {
		t.Errorf("expect get mysql db driver, but get err: %v", err)
	}

	job = NewJob("job1", "postgresql", testPsqlDBDsn)
	_, err = job.getDBDriver()
	if err != nil {
		t.Errorf("expect get postgresql db driver, but get err: %v", err)
	}

	job = NewJob("job1", "x", testDBDsn)
	_, err = job.getDBDriver()
	if err == nil {
		t.Error("expect unsupport database driver err, but actual get nil")
	}
}

func TestWithSshHost(t *testing.T) {
	job := NewJob("job", "mysql", testDBDsn, WithSshHost("localhost"))
	if job.SshHost != "localhost" {
		t.Errorf("expect ssh host: localhost but got: %s", job.SshHost)
	}
}

func TestWithSshUser(t *testing.T) {
	job := NewJob("job", "mysql", testDBDsn, WithSshUser("root"))
	if job.SshUser != "root" {
		t.Errorf("expect ssh user: root but got: %s", job.SshUser)
	}
}

func TestWithGzip(t *testing.T) {
	job := NewJob("job", "mysql", testDBDsn, WithGzip(true))
	if !job.Gzip {
		t.Error("expect gzip but got false")
	}
}

func TestWithSshKey(t *testing.T) {
	job := NewJob("job", "mysql", testDBDsn, WithSshKey("ssh key"))
	if job.SshKey != "ssh key" {
		t.Errorf("expect ssh key: ssh key, but got: %s", job.SshKey)
	}
}

func TestValidateDump(t *testing.T) {
	jobs := make([]*Job, 0)
	job1 := NewJob(
		"job1",
		"mysql",
		testDBDsn,
		WithGzip(true),
		WithDumpOptions("--skip-comments"),
		WithSshKey("====privatekey===="),
		WithSshUser("root"),
		WithSshHost("localhost"),
	)
	jobs = append(jobs, job1)

	dump := Dump{Jobs: jobs}

	err := dump.Validate()
	if err != nil {
		t.Errorf("expected validate dump but got err :%v", err)
	}

	job2 := NewJob("", "mysql", "")
	jobs = append(jobs, job2)
	dump.Jobs = jobs
	err = dump.Validate()

	if !errors.Is(err, ErrMissingJobName) {
		t.Errorf("expected err: %v, actual got: %v", ErrMissingJobName, err)
	}

	job3 := NewJob("job3", "mysql", "")
	jobs = append(jobs, job3)
	dump.Jobs = jobs
	err = dump.Validate()

	if !errors.Is(err, ErrMissingDBDsn) {
		t.Errorf("expected err: %v, actual got: %v", ErrMissingJobName, err)
	}

	job4 := NewJob("job3", "", testDBDsn)
	jobs = append(jobs, job4)
	dump.Jobs = jobs
	err = dump.Validate()

	if !errors.Is(err, ErrMissingDBDriver) {
		t.Errorf("expected err: %v, actual got: %v", ErrMissingJobName, err)
	}
}

func TestResultString(t *testing.T) {
	r1 := &JobResult{
		JobName: "job1",
		Elapsed: time.Second,
	}

	s := r1.String()
	if s != "Job: job1 succeeded, it took 1s" {
		t.Errorf("unexpected string result: %s", s)
	}

	r2 := &JobResult{
		Error:   errors.New("test err"),
		JobName: "job1",
		Elapsed: time.Second,
	}

	s = r2.String()
	if s != "Job: job1 failed, it took 1s with error: test err" {
		t.Errorf("unexpected string result: %s", s)
	}
}

func TestViaSsh(t *testing.T) {
	job := &Job{}

	if job.ViaSsh() != false {
		t.Error("expected false but got true")
	}

	job.SshHost = "mydump.com"
	job.SshUser = "admin"
	job.SshKey = "my-ssh-key"

	if job.ViaSsh() != true {
		t.Error("expected via ssh but got false")
	}
}

func TestGetRunner(t *testing.T) {
	job := &Job{}

	_, err := job.GetRunner()
	if err == nil {
		t.Error("expect error but got nil")
	}

	job.DBDriver = "mysql"
	r, err := job.GetRunner()
	if err != nil {
		t.Error(err)
	}

	if _, ok := r.(*dumper.ExecDumper); !ok {
		t.Errorf("expect exec runner, but got type: %T", r)
	}

	job.DBDriver = "postgresql"
	job.SshHost = "localhost"
	job.SshUser = "admin"
	job.SshKey = "ssh key"
	r, err = job.GetRunner()
	if err != nil {
		t.Error(err)
	}

	if _, ok := r.(*dumper.SshDumper); !ok {
		t.Errorf("expect ssh runner, but got type: %T", r)
	}
}

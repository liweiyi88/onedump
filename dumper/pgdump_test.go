package dumper

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/liweiyi88/onedump/config"
)

var testPsqlDBDsn = "postgres://julianli:julian@localhost:5432/mypsqldb"
var testPsqlRemoteDBDsn = "postgres://julianli:julian@example.com:8888/mypsqldb"

func TestNewPostgreSqlDriver(t *testing.T) {
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)

	if pgdump.Username != "julianli" {
		t.Errorf("expected user: julianli but actual got: %s", pgdump.Username)
	}

	if pgdump.Password != "julian" {
		t.Errorf("expected password: julian but actual got: %s", pgdump.Password)
	}

	if pgdump.Host != "localhost" {
		t.Errorf("expected host: localhost but actual got: %s", pgdump.Host)
	}

	if pgdump.DBName != "mypsqldb" {
		t.Errorf("expected db: mypsqldb but actual got: %s", pgdump.DBName)
	}

	if pgdump.Port != 5432 {
		t.Errorf("expected port: 5432 but actual got: %d", pgdump.Port)
	}

	job = config.NewJob("test", "postgresql", "wrongdsn")
	_, err := NewPgDump(job)
	if err == nil {
		t.Error("expect error but got nil")
	}
}

func TestGetDumpCommandArgs(t *testing.T) {
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)

	args := pgdump.getDumpCommandArgs()

	expect := "--host=localhost --port=5432 --username=julianli --dbname=mypsqldb"
	actual := strings.Join(args, " ")

	if expect != actual {
		t.Errorf("expect :%s, actual: %s", expect, actual)
	}

	job = config.NewJob("remotejob", "postgresql", testPsqlRemoteDBDsn)
	pgdump, _ = NewPgDump(job)

	args = pgdump.getDumpCommandArgs()

	expect = "--host=example.com --port=8888 --username=julianli --dbname=mypsqldb"
	actual = strings.Join(args, " ")

	if expect != actual {
		t.Errorf("expect :%s, actual: %s", expect, actual)
	}
}

func TestPostGreSqlGetExecDumpCommand(t *testing.T) {
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)
	command, args, err := pgdump.getExecDumpCommand()
	if err != nil {
		t.Error(err)
	}

	pgDumpPath, err := exec.LookPath(pgdump.path)
	if err != nil {
		t.Errorf("failed to find pg_dump executable %s %s", pgdump.path, err)
	}

	if command != pgDumpPath {
		t.Errorf("expect command: %s, actual: %s", command, pgDumpPath)
	}

	expect := "--host=localhost --port=5432 --username=julianli --dbname=mypsqldb"
	actual := strings.Join(args, " ")

	if expect != actual {
		t.Errorf("expect :%s, actual: %s", expect, actual)
	}

	pgdump.path = "/wrong"
	_, _, err = pgdump.getExecDumpCommand()
	if err == nil {
		t.Error("expect error but got nil")
	}
}

func TestPostGreSqlGetSshDumpCommand(t *testing.T) {
	job := config.NewJob("test", "postgresql", testPsqlDBDsn, config.WithSshHost("ssh"), config.WithSshKey("key"), config.WithSshUser("user"))
	pgdump, err := NewPgDump(job)
	if err != nil {
		t.Error(err)
	}

	command, err := pgdump.getSshDumpCommand()
	if err != nil {
		t.Error(err)
	}

	expectCommand := "PGPASSWORD=julian pg_dump --host=localhost --port=5432 --username=julianli --dbname=mypsqldb"
	if expectCommand != command {
		t.Errorf("expect: %s, actual got: %s", expectCommand, command)
	}
}

func TestExecDumpEnvironPostgresql(t *testing.T) {
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)
	defer pgdump.close()

	envs, err := pgdump.execDumpEnviron()
	if err != nil {
		t.Error(err)
	}

	if len(envs) != 1 {
		t.Errorf("expect 1 env variable, but got: %d", len(envs))
	}

	if !strings.HasPrefix(envs[0], "PGPASSFILE=") {
		t.Errorf("expect prefix PGPASSFILE= but got: %s", envs[0])
	}
}

func TestCreateCredentialFilePostgresql(t *testing.T) {
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, err := NewPgDump(job)
	if err != nil {
		t.Fatal(err)
	}

	fileName, err := pgdump.createCredentialFile()
	t.Log("create temp credential file", fileName)
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.ReadFile(fileName)

	if err != nil {
		t.Fatal(err)
	}

	actual := string(file)
	expected := "localhost:5432:mypsqldb:julianli:julian"

	if actual != expected {
		t.Errorf("Expected:\n%s \n----should equal to----\n%s", expected, actual)
	}

	err = os.Remove(fileName)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("removed temp credential file", fileName)
}

func TestClosePostgresql(t *testing.T) {
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)
	file1, err := pgdump.createCredentialFile()
	if err != nil {
		t.Error(err)
	}
	_, err = os.Stat(file1)
	if err != nil {
		t.Error(err)
	}

	file2, err := pgdump.createCredentialFile()
	if err != nil {
		t.Error(err)
	}

	_, err = os.Stat(file2)
	if err != nil {
		t.Error(err)
	}

	if len(pgdump.credentialFiles) != 2 {
		t.Errorf("expect 2 credentials files but got: %d", len(pgdump.credentialFiles))
	}

	if err := pgdump.close(); err != nil {
		t.Errorf("could not cleanup psql file: %v", err)
	}

	_, err = os.Stat(file1)
	if !os.IsNotExist(err) {
		t.Errorf("expected file1 not exist error but actual got error: %v", err)
	}

	_, err = os.Stat(file2)
	if !os.IsNotExist(err) {
		t.Errorf("expected file2 not exist error but actual got error: %v", err)
	}

	pgdump.credentialFiles = append(pgdump.credentialFiles, "wrong file")
	if err := pgdump.close(); err == nil {
		t.Error("expect close error, but got nil")
	}
}

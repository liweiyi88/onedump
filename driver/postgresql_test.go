package driver

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

var testPsqlDBDsn = "postgres://julianli:julian@localhost:5432/mypsqldb"

var testPsqlRemoteDBDsn = "postgres://julianli:julian@example.com:8888/mypsqldb"

func TestNewPostgreSqlDriver(t *testing.T) {
	psqlDriver, _ := NewPostgreSqlDriver(testPsqlDBDsn, nil, false)

	if psqlDriver.Username != "julianli" {
		t.Errorf("expected user: julianli but actual got: %s", psqlDriver.Username)
	}

	if psqlDriver.Password != "julian" {
		t.Errorf("expected password: julian but actual got: %s", psqlDriver.Password)
	}

	if psqlDriver.Host != "localhost" {
		t.Errorf("expected host: localhost but actual got: %s", psqlDriver.Host)
	}

	if psqlDriver.DBName != "mypsqldb" {
		t.Errorf("expected db: mypsqldb but actual got: %s", psqlDriver.DBName)
	}

	if psqlDriver.Port != 5432 {
		t.Errorf("expected port: 5432 but actual got: %d", psqlDriver.Port)
	}

	_, err := NewPostgreSqlDriver("wrongdsn", nil, false)
	if err == nil {
		t.Error("expect error but got nil")
	}
}

func TestGetDumpCommandArgs(t *testing.T) {
	psqlDriver, _ := NewPostgreSqlDriver(testPsqlDBDsn, nil, false)

	args := psqlDriver.getDumpCommandArgs()

	expect := "--host=localhost --port=5432 --username=julianli --dbname=mypsqldb"
	actual := strings.Join(args, " ")

	if expect != actual {
		t.Errorf("expect :%s, actual: %s", expect, actual)
	}

	psqlDriver, _ = NewPostgreSqlDriver(testPsqlRemoteDBDsn, nil, false)

	args = psqlDriver.getDumpCommandArgs()

	expect = "--host=example.com --port=8888 --username=julianli --dbname=mypsqldb"
	actual = strings.Join(args, " ")

	if expect != actual {
		t.Errorf("expect :%s, actual: %s", expect, actual)
	}
}

func TestPostGreSqlGetExecDumpCommand(t *testing.T) {
	psqlDriver, _ := NewPostgreSqlDriver(testPsqlDBDsn, nil, false)
	command, args, err := psqlDriver.GetExecDumpCommand()
	if err != nil {
		t.Error(err)
	}

	pgDumpPath, err := exec.LookPath(psqlDriver.PgDumpBinaryPath)
	if err != nil {
		t.Errorf("failed to find pg_dump executable %s %s", psqlDriver.PgDumpBinaryPath, err)
	}

	if command != pgDumpPath {
		t.Errorf("expect command: %s, actual: %s", command, pgDumpPath)
	}

	expect := "--host=localhost --port=5432 --username=julianli --dbname=mypsqldb"
	actual := strings.Join(args, " ")

	if expect != actual {
		t.Errorf("expect :%s, actual: %s", expect, actual)
	}

	psqlDriver.PgDumpBinaryPath = "/wrong"
	_, _, err = psqlDriver.GetExecDumpCommand()
	if err == nil {
		t.Error("expect error but got nil")
	}
}

func TestPostGreSqlGetSshDumpCommand(t *testing.T) {
	psqlDriver, err := NewPostgreSqlDriver(testPsqlDBDsn, nil, false)
	if err != nil {
		t.Error(err)
	}

	command, err := psqlDriver.GetSshDumpCommand()
	if err != nil {
		t.Error(err)
	}

	expectCommand := "PGPASSWORD=julian pg_dump --host=localhost --port=5432 --username=julianli --dbname=mypsqldb"
	if expectCommand != command {
		t.Errorf("expect: %s, actual got: %s", expectCommand, command)
	}
}

func TestExecDumpEnvironPostgresql(t *testing.T) {
	psql, _ := NewPostgreSqlDriver(testPsqlDBDsn, nil, false)
	defer psql.Close()

	envs, err := psql.ExecDumpEnviron()
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
	psql, err := NewPostgreSqlDriver(testPsqlDBDsn, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	fileName, err := psql.createCredentialFile()
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
	psql, _ := NewPostgreSqlDriver(testPsqlDBDsn, nil, false)
	file1, err := psql.createCredentialFile()
	if err != nil {
		t.Error(err)
	}
	_, err = os.Stat(file1)
	if err != nil {
		t.Error(err)
	}

	file2, err := psql.createCredentialFile()
	if err != nil {
		t.Error(err)
	}

	_, err = os.Stat(file2)
	if err != nil {
		t.Error(err)
	}

	if len(psql.credentialFiles) != 2 {
		t.Errorf("expect 2 credentials files but got: %d", len(psql.credentialFiles))
	}

	if err := psql.Close(); err != nil {
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

	psql.credentialFiles = append(psql.credentialFiles, "wrong file")
	if err := psql.Close(); err == nil {
		t.Error("expect close error, but got nil")
	}
}

package driver

import (
	"os/exec"
	"strings"
	"testing"
)

var testPsqlDBDsn = "postgres://julianli:julian@localhost:5432/mypsqldb"

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

	expect := "--host=localhost --username=julianli --dbname=mypsqldb"
	actual := strings.Join(args, " ")

	if expect != actual {
		t.Errorf("expect :%s, actual: %s", expect, actual)
	}
}

func TestPostGreSqlGetDumpCommand(t *testing.T) {
	psqlDriver, _ := NewPostgreSqlDriver(testPsqlDBDsn, nil, false)
	command, args, err := psqlDriver.GetDumpCommand()
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

	expect := "--host=localhost --username=julianli --dbname=mypsqldb"
	actual := strings.Join(args, " ")

	if expect != actual {
		t.Errorf("expect :%s, actual: %s", expect, actual)
	}

	psqlDriver.PgDumpBinaryPath = "/wrong"
	_, _, err = psqlDriver.GetDumpCommand()
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

	expectCommand := "PGPASSWORD=julian pg_dump --host=localhost --username=julianli --dbname=mypsqldb"
	if expectCommand != command {
		t.Errorf("expect: %s, actual got: %s", expectCommand, command)
	}
}

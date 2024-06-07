package dumper

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/liweiyi88/onedump/config"
	"github.com/stretchr/testify/assert"
)

var testPsqlDBDsn = "postgres://julianli:julian@localhost:5432/mypsqldb"
var testPsqlRemoteDBDsn = "postgres://julianli:julian@example.com:8888/mypsqldb"

func TestNewPostgreSqlDriver(t *testing.T) {
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)

	assert := assert.New(t)

	assert.Equal(pgdump.Username, "julianli")
	assert.Equal(pgdump.Password, "julian")
	assert.Equal(pgdump.Host, "localhost")
	assert.Equal(pgdump.DBName, "mypsqldb")
	assert.Equal(pgdump.Port, 5432)

	job = config.NewJob("test", "postgresql", "wrongdsn")
	_, err := NewPgDump(job)
	assert.NotNil(err)
}

func TestGetDumpCommandArgs(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)

	args := pgdump.getDumpCommandArgs()

	expect := "--host=localhost --port=5432 --username=julianli --dbname=mypsqldb"
	actual := strings.Join(args, " ")

	assert.Equal(expect, actual)

	job = config.NewJob("remotejob", "postgresql", testPsqlRemoteDBDsn)
	pgdump, _ = NewPgDump(job)

	args = pgdump.getDumpCommandArgs()

	expect = "--host=example.com --port=8888 --username=julianli --dbname=mypsqldb"
	actual = strings.Join(args, " ")
	assert.Equal(expect, actual)
}

func TestPostGreSqlGetExecDumpCommand(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)

	command, args, err := pgdump.getExecDumpCommand()
	assert.Nil(err)

	pgDumpPath, err := exec.LookPath(pgdump.path)
	assert.Nil(err)
	assert.Equal(command, pgDumpPath)

	expect := "--host=localhost --port=5432 --username=julianli --dbname=mypsqldb"
	actual := strings.Join(args, " ")
	assert.Equal(expect, actual)

	pgdump.path = "/wrong"
	_, _, err = pgdump.getExecDumpCommand()
	assert.NotNil(err)
}

func TestPostGreSqlGetSshDumpCommand(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "postgresql", testPsqlDBDsn, config.WithSshHost("ssh"), config.WithSshKey("key"), config.WithSshUser("user"))
	pgdump, err := NewPgDump(job)
	assert.Nil(err)

	command, err := pgdump.getSshDumpCommand()
	assert.Nil(err)

	expectCommand := "PGPASSWORD=julian pg_dump --host=localhost --port=5432 --username=julianli --dbname=mypsqldb"
	assert.Equal(expectCommand, command)
}

func TestExecDumpEnvironPostgresql(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)
	defer pgdump.close()

	envs, err := pgdump.execDumpEnviron()
	assert.Nil(err)
	assert.Len(envs, 1)
	assert.True(strings.HasPrefix(envs[0], "PGPASSFILE="))
}

func TestCreateCredentialFilePostgresql(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, err := NewPgDump(job)
	assert.Nil(err)

	fileName, err := pgdump.createCredentialFile()
	t.Log("create temp credential file", fileName)
	assert.Nil(err)

	file, err := os.ReadFile(fileName)
	assert.Nil(err)

	actual := string(file)
	expected := "localhost:5432:mypsqldb:julianli:julian"

	assert.Equal(expected, actual)

	err = os.Remove(fileName)
	assert.Nil(err)

	t.Log("removed temp credential file", fileName)
}

func TestClosePostgresql(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "postgresql", testPsqlDBDsn)
	pgdump, _ := NewPgDump(job)

	file1, err := pgdump.createCredentialFile()
	assert.Nil(err)

	_, err = os.Stat(file1)
	assert.Nil(err)

	file2, err := pgdump.createCredentialFile()
	assert.Nil(err)

	_, err = os.Stat(file2)
	assert.Nil(err)

	assert.Len(pgdump.credentialFiles, 2)
	assert.Nil(pgdump.close())

	_, err = os.Stat(file1)

	assert.ErrorIs(err, os.ErrNotExist)

	_, err = os.Stat(file2)
	assert.ErrorIs(err, os.ErrNotExist)

	pgdump.credentialFiles = append(pgdump.credentialFiles, "wrong file")
	assert.NotNil(pgdump.close())
}

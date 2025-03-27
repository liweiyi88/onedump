package dumper

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/liweiyi88/onedump/config"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

var testDBDsn = "admin:my_password@tcp(127.0.0.1:3306)/dump_test"

func TestDefaultGetDumpCommand(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "mysql", testDBDsn)

	mysql, _ := NewMysqlDump(job)
	defer mysql.close()

	args, err := mysql.getDumpCommandArgs()
	assert.Nil(err)

	assert.False(slices.Contains(args, "--no-create-info"))
	assert.True(slices.Contains(args, "--extended-insert"))
	assert.True(slices.Contains(args, "--skip-comments"))
}

func TestGetDumpCommandWithOptions(t *testing.T) {
	t.Run("it should remove db from args when --all-databases option is passed", func(t *testing.T) {
		assert := assert.New(t)
		job := config.NewJob("test", "mysql", testDBDsn, config.WithDumpOptions("--all-databases"))
		mysql, _ := NewMysqlDump(job)
		defer mysql.close()

		args, err := mysql.getDumpCommandArgs()
		assert.Nil(err)

		assert.False(slices.Contains(args, "dump_test"))
		assert.True(slices.Contains(args, "--all-databases"))
	})

	t.Run("it should include db in the args when --all-databases option is not used", func(t *testing.T) {
		assert := assert.New(t)
		job := config.NewJob("test", "mysql", testDBDsn, config.WithDumpOptions("--skip-comments", "--extended-insert", "--no-create-info", "--default-character-set=utf-8", "--single-transaction", "--skip-lock-tables", "--quick", "--set-gtid-purged=ON"))
		mysql, _ := NewMysqlDump(job)
		defer mysql.close()

		args, err := mysql.getDumpCommandArgs()
		assert.Nil(err)

		assert.True(slices.Contains(args, "--no-create-info"))
		assert.True(slices.Contains(args, "dump_test"))
		assert.True(slices.Contains(args, "--extended-insert"))
		assert.True(slices.Contains(args, "--skip-comments"))
		assert.True(slices.Contains(args, "--default-character-set=utf-8"))
		assert.True(slices.Contains(args, "--single-transaction"))
		assert.True(slices.Contains(args, "--skip-lock-tables"))
		assert.True(slices.Contains(args, "--quick"))
		assert.True(slices.Contains(args, "--set-gtid-purged=ON"))
	})

}

func TestCreateCredentialFileMysql(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "mysql", testDBDsn)
	mysql, err := NewMysqlDump(job)

	assert.Nil(err)

	fileName, err := mysql.createCredentialFile()
	t.Log("create temp credential file", fileName)
	assert.Nil(err)

	file, err := os.ReadFile(fileName)
	assert.Nil(err)

	actual := string(file)
	expected := `[client]
user = admin
password = my_password
port = 3306
host = 127.0.0.1`

	assert.Equal(expected, actual)

	err = os.Remove(fileName)
	assert.Nil(err)
	t.Log("removed temp credential file", fileName)
}

func TestGetSshDumpCommand(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "mysql", testDBDsn)
	mysql, _ := NewMysqlDump(job)

	defer mysql.close()

	command, err := mysql.getSshDumpCommand()
	assert.Nil(err)
	assert.True(strings.Contains(command, "mysqldump --defaults-extra-file") && strings.Contains(command, "--skip-comments --extended-insert dump_test"))
}

func TestGetDumpCommand(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "mysql", testDBDsn)
	mysql, _ := NewMysqlDump(job)
	defer mysql.close()

	mysqldumpPath, err := exec.LookPath(mysql.path)
	assert.Nil(err)

	path, args, err := mysql.getExecDumpCommand()
	assert.Nil(err)
	assert.Equal(path, mysqldumpPath)
	assert.Len(args, 4)
}

func TestMysqlGetDumpCommandArgs(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "mysql", testDBDsn, config.WithSshHost("ssh"), config.WithSshKey("key"), config.WithSshUser("user"))
	mysql, _ := NewMysqlDump(job)
	defer mysql.close()

	_, err := exec.LookPath(mysql.path)
	assert.Nil(err)

	_, args, err := mysql.getExecDumpCommand()
	assert.Nil(err)

	expect := "--host 127.0.0.1 --port 3306 -u admin -pmy_password --skip-comments --extended-insert dump_test"
	actual := strings.Join(args, " ")

	assert.Equal(expect, actual)
}

func TestCloseMysql(t *testing.T) {
	assert := assert.New(t)
	job := config.NewJob("test", "mysql", testDBDsn)
	mysql, _ := NewMysqlDump(job)
	file1, err := mysql.createCredentialFile()
	assert.Nil(err)

	_, err = os.Stat(file1)
	assert.Nil(err)

	file2, err := mysql.createCredentialFile()
	assert.Nil(err)

	_, err = os.Stat(file2)
	assert.Nil(err)
	assert.Len(mysql.credentialFiles, 2)
	assert.Nil(mysql.close())

	_, err = os.Stat(file1)
	assert.ErrorIs(err, os.ErrNotExist)

	_, err = os.Stat(file2)
	assert.ErrorIs(err, os.ErrNotExist)

	mysql.credentialFiles = append(mysql.credentialFiles, "wrong file")
	assert.NotNil(mysql.close())
}

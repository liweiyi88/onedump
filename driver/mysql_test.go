package driver

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/exp/slices"
)

var testDBDsn = "admin:my_password@tcp(127.0.0.1:3306)/dump_test"

func TestDefaultGetDumpCommand(t *testing.T) {
	mysql, _ := NewMysqlDriver(testDBDsn, nil, false)
	defer mysql.Close()

	args, err := mysql.getDumpCommandArgs()
	if err != nil {
		t.Fatal(err)
	}

	if slices.Contains(args, "--no-create-info") {
		t.Error("default option should not contain --no-crete-info option")
	}

	if !slices.Contains(args, "--extended-insert") {
		t.Error("default option should include --extended-insert option")
	}

	if !slices.Contains(args, "--skip-comments") {
		t.Error("default option should include --skip-comments option")
	}
}

func TestGetDumpCommandWithOptions(t *testing.T) {
	mysql, _ := NewMysqlDriver(testDBDsn, []string{"--skip-comments", "--extended-insert", "--no-create-info", "--default-character-set=utf-8", "--single-transaction", "--skip-lock-tables", "--quick", "--set-gtid-purged=ON"}, false)
	defer mysql.Close()

	args, err := mysql.getDumpCommandArgs()
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(args, "--no-create-info") {
		t.Error("it should contain --no-crete-info")
	}

	if !slices.Contains(args, "--extended-insert") {
		t.Error("it should contain --extended-insert ")
	}

	if !slices.Contains(args, "--skip-comments") {
		t.Error("it should include --skip-comments option")
	}

	if !slices.Contains(args, "--default-character-set=utf-8") {
		t.Error("it should include --default-character-set=utf-8 option")
	}

	if !slices.Contains(args, "--single-transaction") {
		t.Error("it should include --single-transaction option")
	}

	if !slices.Contains(args, "--skip-lock-tables") {
		t.Error("it should include --skip-lock-tables option")
	}

	if !slices.Contains(args, "--quick") {
		t.Error("it should include --quick option")
	}

	if !slices.Contains(args, "--set-gtid-purged=ON") {
		t.Error("it should include --set-gtid-purged=ON option")
	}
}

func TestCreateCredentialFileMysql(t *testing.T) {
	mysql, err := NewMysqlDriver(testDBDsn, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	fileName, err := mysql.createCredentialFile()
	t.Log("create temp credential file", fileName)
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.ReadFile(fileName)

	if err != nil {
		t.Fatal(err)
	}

	actual := string(file)
	expected := `[client]
user = admin
password = my_password
port = 3306
host = 127.0.0.1`

	if actual != expected {
		t.Errorf("Expected:\n%s \n----should equal to----\n%s", expected, actual)
	}

	err = os.Remove(fileName)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("removed temp credential file", fileName)
}

func TestExecDumpEnviron(t *testing.T) {
	mysql, _ := NewMysqlDriver(testDBDsn, nil, false)
	env, err := mysql.ExecDumpEnviron()
	if err != nil {
		t.Error(err)
	}

	if env != nil {
		t.Errorf("expect nil mysql exec dump env but got %v", env)
	}
}

func TestGetSshDumpCommand(t *testing.T) {
	mysql, _ := NewMysqlDriver(testDBDsn, nil, false)
	defer mysql.Close()

	command, err := mysql.GetSshDumpCommand()
	if err != nil {
		t.Errorf("failed to get dump command %v", command)
	}

	if !strings.Contains(command, "mysqldump --defaults-extra-file") || !strings.Contains(command, "--skip-comments --extended-insert dump_test") {
		t.Errorf("unexpected command: %s", command)
	}

}

func TestGetDumpCommand(t *testing.T) {
	mysql, _ := NewMysqlDriver(testDBDsn, nil, false)
	defer mysql.Close()

	mysqldumpPath, err := exec.LookPath(mysql.MysqlDumpBinaryPath)
	if err != nil {
		t.Fatal(err)
	}

	path, args, err := mysql.GetExecDumpCommand()
	if err != nil {
		t.Error("failed to get dump command")
	}

	if mysqldumpPath != path {
		t.Errorf("expected mysqldump path: %s, actual got: %s", mysqldumpPath, path)
	}

	if len(args) != 4 {
		t.Errorf("get unexpected args, expected %d args, but got: %d", 4, len(args))
	}
}

func TestMysqlGetDumpCommandArgs(t *testing.T) {
	mysql, _ := NewMysqlDriver(testDBDsn, nil, true)
	defer mysql.Close()

	_, err := exec.LookPath(mysql.MysqlDumpBinaryPath)
	if err != nil {
		t.Fatal(err)
	}

	_, args, err := mysql.GetExecDumpCommand()

	if err != nil {
		t.Fatal(err)
	}

	expect := "--host 127.0.0.1 --port 3306 -u admin -p my_password --skip-comments --extended-insert dump_test"
	actual := strings.Join(args, " ")

	if expect != actual {
		t.Errorf("expect :%s, actual: %s", expect, actual)
	}

}

func TestCloseMysql(t *testing.T) {
	mysql, _ := NewMysqlDriver(testDBDsn, nil, false)
	file1, err := mysql.createCredentialFile()
	if err != nil {
		t.Error(err)
	}
	_, err = os.Stat(file1)
	if err != nil {
		t.Error(err)
	}

	file2, err := mysql.createCredentialFile()
	if err != nil {
		t.Error(err)
	}

	_, err = os.Stat(file2)
	if err != nil {
		t.Error(err)
	}

	if len(mysql.credentialFiles) != 2 {
		t.Errorf("expect 2 credentials files but got: %d", len(mysql.credentialFiles))
	}

	if err := mysql.Close(); err != nil {
		t.Errorf("could not cleanup mysql file: %v", err)
	}

	_, err = os.Stat(file1)
	if !os.IsNotExist(err) {
		t.Errorf("expected file1 not exist error but actual got error: %v", err)
	}

	_, err = os.Stat(file2)
	if !os.IsNotExist(err) {
		t.Errorf("expected file2 not exist error but actual got error: %v", err)
	}

	mysql.credentialFiles = append(mysql.credentialFiles, "wrong file")
	if err := mysql.Close(); err == nil {
		t.Error("expect close error, but got nil")
	}
}

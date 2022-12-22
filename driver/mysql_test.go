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
	mysql, err := NewMysqlDriver(testDBDsn, nil, false)
	if err != nil {
		t.Fatal(err)
	}

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
	mysql, err := NewMysqlDriver(testDBDsn, []string{"--skip-comments", "--extended-insert", "--no-create-info", "--default-character-set=utf-8", "--single-transaction", "--skip-lock-tables", "--quick", "--set-gtid-purged=ON"}, false)
	if err != nil {
		t.Fatal(err)
	}

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

func TestCreateCredentialFile(t *testing.T) {
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

func TestGetSshDumpCommand(t *testing.T) {
	mysql, err := NewMysqlDriver(testDBDsn, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	command, err := mysql.GetSshDumpCommand()
	if err != nil {
		t.Errorf("failed to get dump command %v", command)
	}

	if !strings.Contains(command, "mysqldump --defaults-extra-file") || !strings.Contains(command, "--skip-comments --extended-insert dump_test") {
		t.Errorf("unexpected command: %s", command)
	}
}

func TestGetDumpCommand(t *testing.T) {
	mysql, err := NewMysqlDriver(testDBDsn, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	mysqldumpPath, err := exec.LookPath(mysql.MysqlDumpBinaryPath)
	if err != nil {
		t.Fatal(err)
	}

	path, args, err := mysql.GetDumpCommand()
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

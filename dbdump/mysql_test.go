package dbdump

import (
	"os"
	"testing"

	"golang.org/x/exp/slices"
)

func TestDefaultGetDumpCommand(t *testing.T) {
	mysql := NewMysqlDumper("dump_test", "admin", "my_password", "127.0.0.1", 3306, nil, false)

	args, err := mysql.getDumpCommandArgs()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(args)

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
	mysql := NewMysqlDumper("dump_test", "admin", "my_password", "127.0.0.1", 3306, []string{"--skip-comments", "--extended-insert", "--no-create-info", "--default-character-set=utf-8", "--single-transaction", "--skip-lock-tables", "--quick", "--set-gtid-purged=ON"}, false)

	args, err := mysql.getDumpCommandArgs()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(args)

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
	mysql := NewMysqlDumper("dump_test", "admin", "my_password", "127.0.0.1", 3306, nil, false)

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

func TestDump(t *testing.T) {
	mysql := NewMysqlDumper("test_local", "root", "", "127.0.0.1", 3306, nil, false)

	dumpfile, err := os.CreateTemp("", "dbdump")
	if err != nil {
		t.Fatal(err)
	}
	defer dumpfile.Close()

	err = mysql.Dump(dumpfile.Name())
	if err != nil {
		t.Fatal(err)
	}

	out, err := os.ReadFile(dumpfile.Name())
	if err != nil {
		t.Fatal("failed to read the test dump file")
	}

	if len(out) == 0 {
		t.Fatal("test dump file is empty")
	}

	t.Log("test dump file content size", len(out))

	err = os.Remove(dumpfile.Name())
	if err != nil {
		t.Fatal("can not cleanup the test dump file", err)
	}
}

package dumper

import (
	"bytes"
	"testing"
)

type MockDriver struct{}

func (md *MockDriver) GetExecDumpCommand() (string, []string, error) {
	return "date", nil, nil
}

func (md *MockDriver) GetSshDumpCommand() (string, error) {
	return "", nil
}

func (md *MockDriver) ExecDumpEnviron() ([]string, error) {
	return nil, nil
}

func (md *MockDriver) Close() error {
	return nil
}

func TestNewExecDumper(t *testing.T) {
	driver := &MockDriver{}
	execDumper := NewExecDumper(driver)

	if execDumper.DBDriver != driver {
		t.Errorf("unexpected exec dumper %v", execDumper)
	}
}

func TestDump(t *testing.T) {
	buf := make([]byte, 50)
	buffer := bytes.NewBuffer(buf)

	driver := &MockDriver{}
	execDumper := NewExecDumper(driver)
	err := execDumper.Dump(buffer)
	if err != nil {
		t.Errorf("failed to dump to file: %v", err)
	}

	err = execDumper.Dump(buffer)
	if err != nil {
		t.Errorf("failed to dump to file: %v", err)
	}
}

package runner

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

func TestNewExecRunner(t *testing.T) {
	driver := &MockDriver{}
	execRunner := NewExecRunner(true, driver)

	if execRunner.ShouldGzip != true || execRunner.DBDriver != driver {
		t.Errorf("unexpected exec runner %v", execRunner)
	}
}

func TestDumpToFile(t *testing.T) {
	buf := make([]byte, 50)
	buffer := bytes.NewBuffer(buf)

	driver := &MockDriver{}
	execRunner := NewExecRunner(true, driver)
	err := execRunner.DumpToFile(buffer)
	if err != nil {
		t.Errorf("failed to dump to file: %v", err)
	}

	execRunner.ShouldGzip = false
	err = execRunner.DumpToFile(buffer)
	if err != nil {
		t.Errorf("failed to dump to file: %v", err)
	}
}

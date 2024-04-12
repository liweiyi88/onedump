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
	execDumper := NewExecDumper(true, driver)

	if execDumper.ShouldGzip != true || execDumper.DBDriver != driver {
		t.Errorf("unexpected exec dumper %v", execDumper)
	}
}

func TestDumpTo(t *testing.T) {
	buf := make([]byte, 50)
	buffer := bytes.NewBuffer(buf)

	driver := &MockDriver{}
	execDumper := NewExecDumper(true, driver)
	err := execDumper.DumpTo(buffer)
	if err != nil {
		t.Errorf("failed to dump to file: %v", err)
	}

	execDumper.ShouldGzip = false
	err = execDumper.DumpTo(buffer)
	if err != nil {
		t.Errorf("failed to dump to file: %v", err)
	}
}

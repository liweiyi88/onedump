package dumper

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/liweiyi88/onedump/driver"
)

type ExecDumper struct {
	ShouldGzip bool
	DBDriver   driver.Driver
}

func NewExecDumper(shouldGzip bool, driver driver.Driver) *ExecDumper {
	return &ExecDumper{
		ShouldGzip: shouldGzip,
		DBDriver:   driver,
	}
}

func (execDump *ExecDumper) DumpTo(storage io.Writer) error {
	defer func() {
		if err := execDump.DBDriver.Close(); err != nil {
			log.Printf("could not cleanup db driver: %v", err)
		}
	}()

	command, args, err := execDump.DBDriver.GetExecDumpCommand()
	if err != nil {
		return fmt.Errorf("could not to get dump command: %v", err)
	}

	cmd := exec.Command(command, args...)
	envs, err := execDump.DBDriver.ExecDumpEnviron()
	if err != nil {
		return fmt.Errorf("could not get exec dump environment variables: %v", err)
	}

	if len(envs) > 0 {
		cmd.Env = append(os.Environ(), envs...)
	}

	var errBuf strings.Builder

	cmd.Stderr = &errBuf
	cmd.Stdout = storage

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command error: %v, %s", err, errBuf.String())
	}

	return nil
}

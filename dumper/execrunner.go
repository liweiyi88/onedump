package dumper

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"

	"github.com/liweiyi88/onedump/driver"
)

type ExecRunner struct {
	ShouldGzip bool
	DBDriver   driver.Driver
}

func NewExecRunner(shouldGzip bool, driver driver.Driver) *ExecRunner {
	return &ExecRunner{
		ShouldGzip: shouldGzip,
		DBDriver:   driver,
	}
}

func (execDump *ExecRunner) DumpToFile(file io.Writer) error {
	var gzipWriter *gzip.Writer
	if execDump.ShouldGzip {
		gzipWriter = gzip.NewWriter(file)
		defer func() {
			err := gzipWriter.Close()
			if err != nil {
				log.Printf("failed to close gzip writer: %v", err)
			}
		}()
	}

	command, args, err := execDump.DBDriver.GetDumpCommand()
	if err != nil {
		return fmt.Errorf("could not to get dump command: %v", err)
	}

	cmd := exec.Command(command, args...)
	envs := execDump.DBDriver.ExecDumpEnviron()

	if len(envs) > 0 {
		cmd.Env = append(os.Environ(), envs...)
	}

	cmd.Stderr = os.Stderr
	if gzipWriter != nil {
		cmd.Stdout = gzipWriter
	} else {
		cmd.Stdout = file
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command error: %v", err)
	}

	return nil
}

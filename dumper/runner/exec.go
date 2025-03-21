package runner

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

type ExecRunner struct {
	command string
	args    []string
	envs    []string
}

func NewExecRunner(command string, args []string, envs []string) *ExecRunner {
	return &ExecRunner{
		command, args, envs,
	}
}

func (runner *ExecRunner) Run(writer io.Writer) error {
	cmd := exec.Command(runner.command, runner.args...)

	if len(runner.envs) > 0 {
		cmd.Env = append(os.Environ(), runner.envs...)
	}

	var errBuf strings.Builder

	cmd.Stderr = &errBuf
	cmd.Stdout = writer

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command error: %v, %s", err, errBuf.String())
	}

	return nil
}

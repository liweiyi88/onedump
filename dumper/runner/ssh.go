package runner

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/liweiyi88/onedump/dumper/dialer"
)

type SshRunner struct {
	ssh     *dialer.Ssh
	command string
}

func NewSshRunner(sshHost string, sshKey string, sshUser string, command string) *SshRunner {
	ssh := dialer.NewSsh(sshHost, sshKey, sshUser)
	return &SshRunner{
		ssh,
		command,
	}
}

func (runner *SshRunner) Run(writer io.Writer) error {
	client, err := runner.ssh.CreateSshClient()
	if err != nil {
		return fmt.Errorf("failed to dial remote server via ssh: %w", err)
	}

	defer func() {
		// Do not need to call session.Close() here as it will only give EOF error.
		err = client.Close()
		if err != nil {
			log.Printf("failed to close ssh client: %v", err)
		}
	}()

	sshSession, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("failed to start ssh session: %w", err)
	}

	var remoteErr bytes.Buffer

	sshSession.Stderr = &remoteErr
	sshSession.Stdout = writer

	if err := sshSession.Run(runner.command); err != nil {
		return fmt.Errorf("remote command error: %s, %v", remoteErr.String(), err)
	}

	return nil
}

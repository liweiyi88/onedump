package runner

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"

	"golang.org/x/crypto/ssh"
)

type SshRunner struct {
	sshHost string
	sshKey  string
	sshUser string
	command string
}

func NewSshRunner(host, key, user, command string) *SshRunner {
	return &SshRunner{
		sshHost: host,
		sshKey:  key,
		sshUser: user,
		command: command,
	}
}

func ensureHaveSSHPort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "22")
	}
	return addr
}

func (runner *SshRunner) createSshClient() (*ssh.Client, error) {
	host := ensureHaveSSHPort(runner.sshHost)

	signer, err := ssh.ParsePrivateKey([]byte(runner.sshKey))
	if err != nil {
		return nil, fmt.Errorf("failed to create singer :%w", err)
	}

	conf := &ssh.ClientConfig{
		User:            runner.sshUser,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
	}

	return ssh.Dial("tcp", host, conf)
}

func (runner *SshRunner) Run(writer io.Writer) error {
	client, err := runner.createSshClient()
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

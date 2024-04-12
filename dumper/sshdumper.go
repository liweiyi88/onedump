package dumper

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"

	"golang.org/x/crypto/ssh"

	"github.com/liweiyi88/onedump/driver"
)

type SshDumper struct {
	SshHost  string
	SshKey   string
	SshUser  string
	DBDriver driver.Driver
}

func NewSshDumper(host, key, user string, driver driver.Driver) *SshDumper {
	return &SshDumper{
		SshHost:  host,
		SshKey:   key,
		SshUser:  user,
		DBDriver: driver,
	}
}

func ensureHaveSSHPort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "22")
	}
	return addr
}

func (sshDumper *SshDumper) createSshClient() (*ssh.Client, error) {
	host := ensureHaveSSHPort(sshDumper.SshHost)

	signer, err := ssh.ParsePrivateKey([]byte(sshDumper.SshKey))
	if err != nil {
		return nil, fmt.Errorf("failed to create singer :%w", err)
	}

	conf := &ssh.ClientConfig{
		User:            sshDumper.SshUser,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
	}

	return ssh.Dial("tcp", host, conf)
}

func (sshDumper *SshDumper) Dump(storage io.Writer) error {
	client, err := sshDumper.createSshClient()
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
	sshSession.Stdout = storage

	sshCommand, err := sshDumper.DBDriver.GetSshDumpCommand()
	if err != nil {
		return fmt.Errorf("failed to get ssh dump command %w", err)
	}

	if err := sshSession.Run(sshCommand); err != nil {
		return fmt.Errorf("remote command error: %s, %v", remoteErr.String(), err)
	}

	return nil
}

package dumper

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/liweiyi88/onedump/driver"
	"golang.org/x/crypto/ssh"
)

type SshDumper struct {
	SshHost    string
	SshKey     string
	SshUser    string
	ShouldGzip bool
	DBDriver   driver.Driver
}

func NewSshDumper(host, key, user string, shouldGzip bool, driver driver.Driver) *SshDumper {
	return &SshDumper{
		SshHost:    host,
		SshKey:     key,
		SshUser:    user,
		ShouldGzip: shouldGzip,
		DBDriver:   driver,
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

func (sshDumper *SshDumper) DumpToFile(file io.Writer) error {
	var gzipWriter *gzip.Writer
	if sshDumper.ShouldGzip {
		gzipWriter = gzip.NewWriter(file)
		defer func() {
			err := gzipWriter.Close()
			if err != nil {
				log.Printf("failed to close gzip writer: %v", err)
			}
		}()
	}

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
	if gzipWriter != nil {
		sshSession.Stdout = gzipWriter
	} else {
		sshSession.Stdout = file
	}

	sshCommand, err := sshDumper.DBDriver.GetSshDumpCommand()
	if err != nil {
		return fmt.Errorf("failed to get ssh dump command %w", err)
	}

	if err := sshSession.Run(sshCommand); err != nil {
		return fmt.Errorf("remote command error: %s, %v", remoteErr.String(), err)
	}

	return nil
}

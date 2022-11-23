package dump

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"

	"golang.org/x/crypto/ssh"
)

type SshDumper struct {
	User           string
	Host           string
	PrivateKeyFile string
}

func NewSshDumper(host, user, privateKeyFile string) *SshDumper {
	return &SshDumper{
		Host:           host,
		User:           user,
		PrivateKeyFile: privateKeyFile,
	}
}

func (sshDumper *SshDumper) Dump(dumpFile, command string, shouldGzip bool) error {
	defer trace("ssh dump")()

	host := ensureHavePort(sshDumper.Host)

	pKey, err := os.ReadFile(sshDumper.PrivateKeyFile)
	if err != nil {
		return fmt.Errorf("can not read the private key file :%w", err)
	}

	signer, err := ssh.ParsePrivateKey(pKey)
	if err != nil {
		return fmt.Errorf("failed to create singer :%w", err)
	}

	conf := &ssh.ClientConfig{
		User:            sshDumper.User,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
	}

	client, err := ssh.Dial("tcp", host, conf)
	if err != nil {
		return fmt.Errorf("failed to dial remote server via ssh %w", err)
	}

	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		log.Fatalln("failed to start session: ", err)
	}

	defer session.Close()

	file, gzipWriter, err := dumpWriters(dumpFile, shouldGzip)
	if err != nil {
		return fmt.Errorf("failed to get dump writers %w", err)
	}

	if shouldGzip {
		session.Stdout = gzipWriter
	} else {
		session.Stdout = file
	}

	var remoteErr bytes.Buffer
	session.Stderr = &remoteErr

	if err := session.Run(command); err != nil {
		return fmt.Errorf("remote command error: %s, %v", remoteErr.String(), err)
	}

	// If it is gzip, we should firstly close the gzipWriter then close the file.
	if gzipWriter != nil {
		gzipWriter.Close()
	}

	file.Close()

	log.Printf("file has been successfully dumped to %s", file.Name())

	return nil
}

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "22")
	}
	return addr
}

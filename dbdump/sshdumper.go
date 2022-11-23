package dbdump

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"golang.org/x/crypto/ssh"
)

type SshDumper struct {
	User           string
	Host           string
	PrivateKeyFile string
	DbType         string
}

func NewSshDumper(host, user, privateKeyFile string) *SshDumper {
	return &SshDumper{
		Host:           host,
		User:           user,
		PrivateKeyFile: privateKeyFile,
	}
}

func (sshDumper *SshDumper) Dump(dumpFile, command string) error {
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

	dumpWriter, err := dumpWriter(dumpFile)
	if err != nil {
		return err
	}

	defer dumpWriter.Close()

	var remoteErr bytes.Buffer

	session.Stdout = dumpWriter
	session.Stderr = &remoteErr

	if err := session.Run(command); err != nil {
		return fmt.Errorf("remote command error: %s, %v", remoteErr.String(), err)
	}

	log.Printf("file has been successfully dumped to %s", dumpFile)

	return nil
}

func dumpWriter(dumpFile string) (io.WriteCloser, error) {
	//TODO:check if dumpFile has s3 prefix, if not use local file system
	file, err := os.Create(dumpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create dump file %w", err)
	}

	return file, nil
}

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "22")
	}
	return addr
}

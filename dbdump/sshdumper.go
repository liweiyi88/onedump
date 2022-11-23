package dbdump

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
		log.Fatalln("failed to dial: ", err)
	}

	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		log.Fatalln("failed to start session: ", err)
	}

	defer session.Close()

	var remoteOut bytes.Buffer
	var remoteErr bytes.Buffer

	session.Stdout = &remoteOut
	session.Stderr = &remoteErr

	if err := session.Run(command); err != nil {
		log.Fatal(remoteErr.String())
	}

	fmt.Println(remoteOut.String())

	//@TODO if provide download option, we download the db to local.

	return nil
}

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "22")
	}
	return addr
}

package dump

import (
	"fmt"
	"log"
	"net"
	"os"

	"golang.org/x/crypto/ssh"
)

type SshDumper struct {
	User, Host, PrivateKeyFile string
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

	err = dump(session, dumpFile, shouldGzip, command)
	if err != nil {
		return err
	}

	return nil
}

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "22")
	}
	return addr
}

package dialer

import (
	"fmt"
	"net"

	"golang.org/x/crypto/ssh"
)

type Ssh struct {
	host string
	key  string
	user string
}

func NewSsh(host, key, user string) *Ssh {
	return &Ssh{
		host,
		key,
		user,
	}
}

func ensureHaveSSHPort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "22")
	}
	return addr
}

func (s *Ssh) CreateSshClient() (*ssh.Client, error) {
	host := ensureHaveSSHPort(s.host)

	signer, err := ssh.ParsePrivateKey([]byte(s.key))
	if err != nil {
		return nil, fmt.Errorf("failed to create ssh singer :%w", err)
	}

	conf := &ssh.ClientConfig{
		User:            s.user,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
	}

	return ssh.Dial("tcp", host, conf)
}

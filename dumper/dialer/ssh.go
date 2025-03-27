package dialer

import (
	"encoding/base64"
	"fmt"
	"net"
	"os"

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

	privateKey, err := parseSSHKey(s.key)

	if err != nil {
		return nil, fmt.Errorf("fail to parse SSH key, error: %v", err)
	}

	signer, err := ssh.ParsePrivateKey(privateKey)
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

func parseSSHKey(key string) ([]byte, error) {
	_, err := os.Stat(key)

	if err == nil {
		return os.ReadFile(key)
	}

	// Try to decode by base64 encoding
	decoded, err := base64.StdEncoding.DecodeString(key)

	// If it is not base64 encoded, then we jsut return the original key.
	if err != nil {
		return []byte(key), nil
	}

	return decoded, nil
}

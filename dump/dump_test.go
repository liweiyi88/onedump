package dump

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"testing"
)

func generateTestRSAPrivatePEMFile() (string, error) {
	tempDir := os.TempDir()

	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return "", fmt.Errorf("could not genereate rsa key pair %w", err)
	}

	keyPEM := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(key),
		},
	)

	privatePEMFile := fmt.Sprintf("%s/%s", tempDir, "sshdump_test.rsa")

	if err := os.WriteFile(privatePEMFile, keyPEM, 0700); err != nil {
		return "", fmt.Errorf("failed to write private key to file %w", err)
	}

	return privatePEMFile, nil
}

func TestEnsureSSHHostHavePort(t *testing.T) {
	sshHost := "127.0.0.1"

	if ensureHaveSSHPort(sshHost) != sshHost+":22" {
		t.Error("ssh host port is not ensured")
	}
}

// func TestNewSshDumper(t *testing.T) {
// 	sshDumper := NewSshDumper("127.0.0.1", "root", "~/.ssh/test.pem")

// 	if sshDumper.Host != "127.0.0.1" {
// 		t.Errorf("ssh host is unexpected, exepct: %s, actual: %s", "127.0.0.1", sshDumper.Host)
// 	}

// 	if sshDumper.User != "root" {
// 		t.Errorf("ssh user is unexpected, exepct: %s, actual: %s", "root", sshDumper.User)
// 	}

// 	if sshDumper.PrivateKeyFile != "~/.ssh/test.pem" {
// 		t.Errorf("ssh private key file path is unexpected, exepct: %s, actual: %s", "~/.ssh/test.pem", sshDumper.PrivateKeyFile)
// 	}
// }

// func TestSSHDump(t *testing.T) {
// 	privateKeyFile, err := generateTestRSAPrivatePEMFile()

// 	dumpFile := os.TempDir() + "sshdump.sql.gz"

// 	if err != nil {
// 		t.Error("failed to generate test rsa key pairs", err)
// 	}

// 	defer func() {
// 		err := os.Remove(privateKeyFile)
// 		if err != nil {
// 			t.Logf("failed to remove private key file %s", privateKeyFile)
// 		}
// 	}()

// 	go func() {
// 		sshDumper := NewSshDumper("127.0.0.1:2022", "root", privateKeyFile)
// 		err := sshDumper.Dump(dumpFile, "echo hello", true)
// 		if err != nil {
// 			t.Error("failed to dump file", err)
// 		}
// 	}()

// 	// An SSH server is represented by a ServerConfig, which holds
// 	// certificate details and handles authentication of ServerConns.
// 	config := &ssh.ServerConfig{
// 		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
// 			return &ssh.Permissions{
// 				// Record the public key used for authentication.
// 				Extensions: map[string]string{
// 					"pubkey-fp": ssh.FingerprintSHA256(pubKey),
// 				},
// 			}, nil
// 		},
// 	}

// 	privateBytes, err := os.ReadFile(privateKeyFile)
// 	if err != nil {
// 		t.Fatal("Failed to load private key: ", err)
// 	}

// 	private, err := ssh.ParsePrivateKey(privateBytes)
// 	if err != nil {
// 		t.Fatal("Failed to parse private key: ", err)
// 	}

// 	config.AddHostKey(private)

// 	// Once a ServerConfig has been configured, connections can be
// 	// accepted.
// 	listener, err := net.Listen("tcp", "0.0.0.0:2022")
// 	if err != nil {
// 		t.Fatal("failed to listen for connection: ", err)
// 	}

// 	nConn, err := listener.Accept()
// 	if err != nil {
// 		t.Fatal("failed to accept incoming connection: ", err)
// 	}

// 	// Before use, a handshake must be performed on the incoming
// 	// net.Conn.
// 	conn, chans, reqs, err := ssh.NewServerConn(nConn, config)
// 	if err != nil {
// 		log.Fatal("failed to handshake: ", err)
// 	}
// 	t.Logf("logged in with key %s", conn.Permissions.Extensions["pubkey-fp"])

// 	// The incoming Request channel must be serviced.
// 	go ssh.DiscardRequests(reqs)

// 	// Service the incoming Channel channel.
// 	newChannel := <-chans
// 	// Channels have a type, depending on the application level
// 	// protocol intended. In the case of a shell, the type is
// 	// "session" and ServerShell may be used to present a simple
// 	// terminal interface.
// 	if newChannel.ChannelType() != "session" {
// 		newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
// 		t.Fatal("unknown channel type")
// 	}

// 	channel, requests, err := newChannel.Accept()

// 	if err != nil {
// 		log.Fatalf("Could not accept channel: %v", err)
// 	}

// 	req := <-requests
// 	req.Reply(true, []byte("ssh dump"))
// 	channel.SendRequest("exit-status", false, []byte{0, 0, 0, 0})

// 	channel.Close()
// 	conn.Close()

// 	if _, err := os.Stat(dumpFile); errors.Is(err, os.ErrNotExist) {
// 		t.Error("dump file does not existed")
// 	} else {
// 		err := os.Remove(dumpFile)
// 		if err != nil {
// 			t.Fatal("failed to remove the test dump file", err)
// 		}
// 	}
// }

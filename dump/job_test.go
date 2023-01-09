package dump

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"testing"

	"github.com/liweiyi88/onedump/storage/local"
	"golang.org/x/crypto/ssh"
)

var testDBDsn = "root@tcp(127.0.0.1:3306)/dump_test"

func generateRSAPrivateKey() (string, error) {
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

	return string(keyPEM), nil
}

func TestEnsureSSHHostHavePort(t *testing.T) {
	sshHost := "127.0.0.1"

	if ensureHaveSSHPort(sshHost) != sshHost+":22" {
		t.Error("ssh host port is not ensured")
	}

	sshHost = "127.0.0.1:22"
	actual := ensureHaveSSHPort(sshHost)
	if actual != sshHost {
		t.Errorf("expect ssh host: %s, actual: %s", sshHost, actual)
	}
}

func TestGetDBDriver(t *testing.T) {
	job := NewJob("job1", "mysql", "test.sql", testDBDsn)

	_, err := job.getDBDriver()
	if err != nil {
		t.Errorf("expect get mysql db driver, but get err: %v", err)
	}

	job = NewJob("job1", "x", "test.sql", testDBDsn)
	_, err = job.getDBDriver()
	if err == nil {
		t.Error("expect unsupport database driver err, but actual get nil")
	}
}

func TestDumpValidate(t *testing.T) {
	jobs := make([]*Job, 0)
	job1 := NewJob(
		"job1",
		"mysql",
		"test.sql",
		testDBDsn,
		WithGzip(true),
		WithDumpOptions("--skip-comments"),
		WithSshKey("====privatekey===="),
		WithSshUser("root"),
		WithSshHost("localhost"),
	)
	jobs = append(jobs, job1)

	dump := Dump{Jobs: jobs}

	err := dump.Validate()
	if err != nil {
		t.Errorf("expected validate dump but got err :%v", err)
	}

	job2 := NewJob("", "mysql", "dump.sql", "")
	jobs = append(jobs, job2)
	dump.Jobs = jobs
	err = dump.Validate()

	if !errors.Is(err, ErrMissingJobName) {
		t.Errorf("expected err: %v, actual got: %v", ErrMissingJobName, err)
	}

	job3 := NewJob("job3", "mysql", "dump.sql", "")
	jobs = append(jobs, job3)
	dump.Jobs = jobs
	err = dump.Validate()

	if !errors.Is(err, ErrMissingDBDsn) {
		t.Errorf("expected err: %v, actual got: %v", ErrMissingJobName, err)
	}

	job4 := NewJob("job3", "", "dump.sql", testDBDsn)
	jobs = append(jobs, job4)
	dump.Jobs = jobs
	err = dump.Validate()

	if !errors.Is(err, ErrMissingDBDriver) {
		t.Errorf("expected err: %v, actual got: %v", ErrMissingJobName, err)
	}
}

func TestRun(t *testing.T) {
	tempDir := os.TempDir()
	privateKey, err := generateRSAPrivateKey()
	if err != nil {
		t.Errorf("failed to generate test private key %v", err)
	}

	jobs := make([]*Job, 0, 1)
	sshJob := NewJob("ssh", "mysql", tempDir+"/test.sql", testDBDsn, WithSshHost("127.0.0.1:2022"), WithSshUser("root"), WithSshKey(privateKey))
	localStorages := make([]*local.Local, 0)
	dumpFile := os.TempDir() + "hello.sql"
	localStorages = append(localStorages, &local.Local{Path: dumpFile})

	sshJob.Storage.Local = localStorages

	jobs = append(jobs, sshJob)
	dump := Dump{Jobs: jobs}

	go func(dump Dump) {
		for _, job := range dump.Jobs {
			job.Run()
		}
	}(dump)

	// An SSH server is represented by a ServerConfig, which holds
	// certificate details and handles authentication of ServerConns.
	config := &ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			return &ssh.Permissions{
				// Record the public key used for authentication.
				Extensions: map[string]string{
					"pubkey-fp": ssh.FingerprintSHA256(pubKey),
				},
			}, nil
		},
	}

	private, err := ssh.ParsePrivateKey([]byte(privateKey))
	if err != nil {
		t.Fatal("Failed to parse private key: ", err)
	}

	config.AddHostKey(private)

	// Once a ServerConfig has been configured, connections can be
	// accepted.
	listener, err := net.Listen("tcp", "0.0.0.0:2022")
	if err != nil {
		t.Fatal("failed to listen for connection: ", err)
	}

	nConn, err := listener.Accept()
	if err != nil {
		t.Fatal("failed to accept incoming connection: ", err)
	}

	// Before use, a handshake must be performed on the incoming
	// net.Conn.
	conn, chans, reqs, err := ssh.NewServerConn(nConn, config)
	if err != nil {
		log.Fatal("failed to handshake: ", err)
	}
	t.Logf("logged in with key %s", conn.Permissions.Extensions["pubkey-fp"])

	// The incoming Request channel must be serviced.
	go ssh.DiscardRequests(reqs)

	// Service the incoming Channel channel.
	newChannel := <-chans
	// Channels have a type, depending on the application level
	// protocol intended. In the case of a shell, the type is
	// "session" and ServerShell may be used to present a simple
	// terminal interface.
	if newChannel.ChannelType() != "session" {
		newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
		t.Fatal("unknown channel type")
	}

	channel, requests, err := newChannel.Accept()

	if err != nil {
		log.Fatalf("Could not accept channel: %v", err)
	}

	req := <-requests
	req.Reply(true, []byte("ssh dump"))
	channel.SendRequest("exit-status", false, []byte{0, 0, 0, 0})

	channel.Close()
	conn.Close()

	if _, err := os.Stat(dumpFile); errors.Is(err, os.ErrNotExist) {
		t.Error("dump file does not existed")
	} else {
		err := os.Remove(dumpFile)
		if err != nil {
			t.Fatal("failed to remove the test dump file", err)
		}
	}
}

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

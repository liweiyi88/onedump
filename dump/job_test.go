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
	"time"

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
	job := NewJob("job1", "mysql", testDBDsn)

	_, err := job.getDBDriver()
	if err != nil {
		t.Errorf("expect get mysql db driver, but get err: %v", err)
	}

	job = NewJob("job1", "x", testDBDsn)
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

	job2 := NewJob("", "mysql", "")
	jobs = append(jobs, job2)
	dump.Jobs = jobs
	err = dump.Validate()

	if !errors.Is(err, ErrMissingJobName) {
		t.Errorf("expected err: %v, actual got: %v", ErrMissingJobName, err)
	}

	job3 := NewJob("job3", "mysql", "")
	jobs = append(jobs, job3)
	dump.Jobs = jobs
	err = dump.Validate()

	if !errors.Is(err, ErrMissingDBDsn) {
		t.Errorf("expected err: %v, actual got: %v", ErrMissingJobName, err)
	}

	job4 := NewJob("job3", "", testDBDsn)
	jobs = append(jobs, job4)
	dump.Jobs = jobs
	err = dump.Validate()

	if !errors.Is(err, ErrMissingDBDriver) {
		t.Errorf("expected err: %v, actual got: %v", ErrMissingJobName, err)
	}
}

func TestRun(t *testing.T) {
	privateKey, err := generateRSAPrivateKey()
	if err != nil {
		t.Errorf("failed to generate test private key %v", err)
	}

	jobs := make([]*Job, 0, 1)
	sshJob := NewJob("ssh", "mysql", testDBDsn, WithSshHost("127.0.0.1:20001"), WithSshUser("root"), WithSshKey(privateKey))
	localStorages := make([]*local.Local, 0)

	dir, _ := os.Getwd()
	dumpFile := dir + "/hello.sql"

	t.Logf("dump file: %s", dumpFile)

	localStorages = append(localStorages, &local.Local{Path: dumpFile})

	sshJob.Storage.Local = localStorages

	jobs = append(jobs, sshJob)
	dump := Dump{Jobs: jobs}

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
	listener, err := net.Listen("tcp", "0.0.0.0:20001")
	if err != nil {
		t.Fatal("failed to listen for connection: ", err)
	}

	finishCh := make(chan struct{})
	go func(dump Dump) {
		for _, job := range dump.Jobs {
			job.Run()
			finishCh <- struct{}{}
		}
	}(dump)

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

	<-finishCh
	if _, err := os.Stat(dumpFile); errors.Is(err, os.ErrNotExist) {
		t.Error("dump file does not existed")
	} else {
		err := os.Remove(dumpFile)
		if err != nil {
			t.Fatal("failed to remove the test dump file", err)
		}
	}
}

func TestResultString(t *testing.T) {
	r1 := &JobResult{
		JobName: "job1",
		Elapsed: time.Second,
	}

	s := r1.String()
	if s != "Job: job1 succeeded, it took 1s" {
		t.Errorf("unexpected string result: %s", s)
	}

	r2 := &JobResult{
		Error:   errors.New("test err"),
		JobName: "job1",
		Elapsed: time.Second,
	}

	s = r2.String()
	if s != "Job: job1 failed, it took 1s with error: test err" {
		t.Errorf("unexpected string result: %s", s)
	}
}

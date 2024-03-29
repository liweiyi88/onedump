package handler

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/dumper"
	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/storage/dropbox"
	"github.com/liweiyi88/onedump/storage/gdrive"
	"github.com/liweiyi88/onedump/storage/local"
	"github.com/liweiyi88/onedump/storage/s3"
)

var testDBDsn = "root@tcp(127.0.0.1:3306)/dump_test"
var testPsqlDBDsn = "postgres://julianli:julian@localhost:5432/mypsqldb"

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

func TestUploadCacheDir(t *testing.T) {
	actual := cacheFileDir()

	workDir, _ := os.Getwd()
	prefix := fmt.Sprintf("%s/%s", workDir, cacheDirPrefix)

	if !strings.HasPrefix(actual, prefix) {
		t.Errorf("get unexpected cache dir: expected: %s, actual: %s", prefix, actual)
	}
}

func TestGenerateCacheFileName(t *testing.T) {
	expectedLen := 5
	name := fileutil.GenerateRandomName(expectedLen)

	actualLen := len([]rune(name))
	if actualLen != expectedLen {
		t.Errorf("unexpected cache filename, expected length: %d, actual length: %d", 5, actualLen)
	}
}

func TestUploadCacheFilePath(t *testing.T) {

	cacheDir := cacheFileDir()

	gziped := cacheFilePath(cacheDir, true)

	if !strings.HasSuffix(gziped, ".gz") {
		t.Errorf("expected filename has .gz extension, actual file name: %s", gziped)
	}

	sql := cacheFilePath(cacheDir, false)

	if !strings.HasSuffix(sql, ".sql") {
		t.Errorf("expected filename has .sql extension, actual file name: %s", sql)
	}

	sql2 := cacheFilePath(cacheDir, false)

	if sql == sql2 {
		t.Errorf("expected unique file name but got same filename %s", sql)
	}
}

type mockDumper struct {
}

func (mockDumper *mockDumper) DumpToFile(file io.Writer) error {
	return nil
}

type mockErrorDumper struct {
}

func (mockDumper *mockErrorDumper) DumpToFile(file io.Writer) error {
	return errors.New("mock dumper err")
}

func TestDumpToCacheFile(t *testing.T) {
	dumper := &mockDumper{}
	jobHandler := &JobHandler{
		Job: &config.Job{},
	}

	_, cacheDir1, err := jobHandler.dumpToCacheFile(dumper)
	if err != nil {
		t.Error(err)
	}

	er := &mockErrorDumper{}
	_, cacheDir2, err := jobHandler.dumpToCacheFile(er)
	if err == nil {
		t.Error("expect error but got nil")
	}

	defer func() {
		err := os.RemoveAll(cacheDir1)
		if err != nil {
			log.Println("failed to remove cache dir after dump", err)
		}

		err = os.RemoveAll(cacheDir2)
		if err != nil {
			log.Println("failed to remove cache dir after dump", err)
		}
	}()
}

func TestDo(t *testing.T) {
	privateKey, err := generateRSAPrivateKey()
	if err != nil {
		t.Errorf("failed to generate test private key %v", err)
	}

	jobs := make([]*config.Job, 0, 1)
	sshJob := config.NewJob("ssh", "mysql", testDBDsn, config.WithSshHost("127.0.0.1:20001"), config.WithSshUser("root"), config.WithSshKey(privateKey))
	localStorages := make([]*local.Local, 0)

	dir, _ := os.Getwd()
	dumpFile := dir + "/hello.sql"

	t.Logf("dump file: %s", dumpFile)

	localStorages = append(localStorages, &local.Local{Path: dumpFile})

	sshJob.Storage.Local = localStorages

	jobs = append(jobs, sshJob)
	onedump := config.Dump{Jobs: jobs}

	// An SSH server is represented by a ServerConfig, which holds
	// certificate details and handles authentication of ServerConns.
	sshConfig := &ssh.ServerConfig{
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

	sshConfig.AddHostKey(private)

	// Once a ServerConfig has been configured, connections can be
	// accepted.
	listener, err := net.Listen("tcp", "0.0.0.0:20001")
	if err != nil {
		t.Fatal("failed to listen for connection: ", err)
	}

	finishCh := make(chan struct{})
	go func(onedump config.Dump) {
		for _, job := range onedump.Jobs {
			NewJobHandler(job).Do()
			finishCh <- struct{}{}
		}
	}(onedump)

	nConn, err := listener.Accept()
	if err != nil {
		t.Fatal("failed to accept incoming connection: ", err)
	}

	// Before use, a handshake must be performed on the incoming
	// net.Conn.
	conn, chans, reqs, err := ssh.NewServerConn(nConn, sshConfig)
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

func TestGetStorages(t *testing.T) {

	localStore := local.Local{Path: "db_backup/onedump.sql"}
	s3 := s3.NewS3("mybucket", "key", "", "", "")
	gdrive := &gdrive.GDrive{
		FileName: "mydump",
		FolderId: "",
	}

	dropbox := &dropbox.Dropbox{
		RefreshToken: "token",
	}

	job := &config.Job{}
	job.Storage.Local = append(job.Storage.Local, &localStore)
	job.Storage.S3 = append(job.Storage.S3, s3)
	job.Storage.GDrive = append(job.Storage.GDrive, gdrive)
	job.Storage.Dropbox = append(job.Storage.Dropbox, dropbox)

	jobHandler := NewJobHandler(job)

	if len(jobHandler.getStorages()) != 4 {
		t.Errorf("expecte 4 storage but actual got: %d", len(jobHandler.getStorages()))
	}
}

func TestEnsureFileSuffix(t *testing.T) {
	gzip := fileutil.EnsureFileSuffix("test.sql", true)
	if gzip != "test.sql.gz" {
		t.Errorf("expected filename has .gz extension, actual file name: %s", gzip)
	}

	sql := fileutil.EnsureFileSuffix("test.sql.gz", true)

	if sql != "test.sql.gz" {
		t.Errorf("expected: %s is not equals to actual: %s", sql, "test.sql.gz")
	}
}

func TestCreateCacheFile(t *testing.T) {
	file, cacheDir, _ := createCacheFile(true)

	defer func() {
		file.Close()

		err := os.RemoveAll(cacheDir)
		if err != nil {
			log.Println("failed to remove cache dir after dump", err)
		}
	}()

	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		t.Errorf("failed to get cache file info %v", err)
	}

	if fileInfo.Size() != 0 {
		t.Errorf("expected empty file but get size: %d", fileInfo.Size())
	}
}

func TestGetDumper(t *testing.T) {
	job := &config.Job{}
	jobHandler := NewJobHandler(job)

	_, err := jobHandler.getDumper()
	if err == nil {
		t.Error("expect error but got nil")
	}

	job.DBDriver = "mysql"
	r, err := jobHandler.getDumper()
	if err != nil {
		t.Error(err)
	}

	if _, ok := r.(*dumper.ExecDumper); !ok {
		t.Errorf("expect exec dumper, but got type: %T", r)
	}

	job.DBDriver = "postgresql"
	job.SshHost = "localhost"
	job.SshUser = "admin"
	job.SshKey = "ssh key"
	r, err = jobHandler.getDumper()
	if err != nil {
		t.Error(err)
	}

	if _, ok := r.(*dumper.SshDumper); !ok {
		t.Errorf("expect ssh dumper, but got type: %T", r)
	}
}

func TestGetDBDriver(t *testing.T) {
	job := config.NewJob("job1", "mysql", testDBDsn)
	jobHandler := NewJobHandler(job)

	_, err := jobHandler.getDBDriver()
	if err != nil {
		t.Errorf("expect get mysql db driver, but get err: %v", err)
	}

	job = config.NewJob("job1", "postgresql", testPsqlDBDsn)
	jobHandler = NewJobHandler(job)
	_, err = jobHandler.getDBDriver()
	if err != nil {
		t.Errorf("expect get postgresql db driver, but get err: %v", err)
	}

	job = config.NewJob("job1", "x", testDBDsn)
	jobHandler = NewJobHandler(job)
	_, err = jobHandler.getDBDriver()
	if err == nil {
		t.Error("expect unsupport database driver err, but actual get nil")
	}
}

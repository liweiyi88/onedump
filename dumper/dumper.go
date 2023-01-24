package dumper

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/liweiyi88/onedump/driver"
)

const cacheDirPrefix = ".onedump"

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Dumper struct {
	SshDump    bool
	DBDriver   driver.Driver
	SshHost    string
	SshKey     string
	SshUser    string
	ShouldGzip bool
}

func NewDumper(sshDumper bool, driver driver.Driver, sshHost, sshKey, sshUser string, shouldGzip bool) *Dumper {
	return &Dumper{
		SshDump:    sshDumper,
		DBDriver:   driver,
		SshHost:    sshHost,
		SshKey:     sshKey,
		SshUser:    sshUser,
		ShouldGzip: shouldGzip,
	}
}

func generateRandomName(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// For uploading dump file to remote storage, we need to firstly dump the db content to a dir locally.
// We firstly try to get current work dir, if not successful, then try to get home dir and finally try temp dir.
// Be aware of the size limit of a temp dir in different OS.
func cacheFileDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Printf("Cannot get the current directory: %v, using $HOME directory!", err)
		dir, err = os.UserHomeDir()
		if err != nil {
			log.Printf("Cannot get the user home directory: %v, using /tmp directory!", err)
			dir = os.TempDir()
		}
	}

	// randomise the upload cache dir, otherwise we will have race condition when have more than one dump jobs.
	return fmt.Sprintf("%s/%s%s", dir, cacheDirPrefix, generateRandomName(4))
}

// Ensure a file has proper file extension.
func ensureFileSuffix(filename string, shouldGzip bool) string {
	if !shouldGzip {
		return filename
	}

	if strings.HasSuffix(filename, ".gz") {
		return filename
	}

	return filename + ".gz"
}

func cacheFilePath(cacheDir string, shouldGzip bool) string {
	filename := fmt.Sprintf("%s/%s", cacheDir, generateRandomName(8)+".sql")
	return ensureFileSuffix(filename, shouldGzip)
}

func CreateCacheFile(gzip bool) (*os.File, string, error) {
	cacheDir := cacheFileDir()
	err := os.MkdirAll(cacheDir, 0750)

	if err != nil {
		return nil, "", fmt.Errorf("failed to create cache dir for remote upload. %w", err)
	}

	dumpFileName := cacheFilePath(cacheDir, gzip)

	file, err := os.Create(dumpFileName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create cache file: %w", err)
	}

	return file, cacheDir, nil
}

// Ensure a file has unique name when necessary.
func ensureUniqueness(path string, unique bool) string {
	if !unique {
		return path
	}

	s := strings.Split(path, "/")

	filename := s[len(s)-1]
	now := time.Now().UTC().Format("20060102150405")
	uniqueFile := now + "-" + filename

	s[len(s)-1] = uniqueFile

	return strings.Join(s, "/")
}

func EnsureFileName(path string, shouldGzip, unique bool) string {
	p := ensureFileSuffix(path, shouldGzip)
	return ensureUniqueness(p, unique)
}

func (dumper *Dumper) DumpToCacheFile() (string, func(), error) {
	file, cacheDir, err := CreateCacheFile(dumper.ShouldGzip)

	cleanup := func() {
		err := os.RemoveAll(cacheDir)
		if err != nil {
			log.Println("failed to remove cache dir after dump", err)
		}
	}

	if err != nil {
		return "", cleanup, err
	}

	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close cache file: %v", err)
		}
	}()

	err = dumper.dumpToFile(file)
	if err != nil {
		return "", cleanup, fmt.Errorf("failed to dump content to file: %w,", err)
	}

	// We have to close the file in defer function and returns filename instead of returing the fd (os.File)
	// Otherwise if we pass the fd and the storage func reuse the same fd, the file will be corrupted.
	return file.Name(), cleanup, nil
}

func (dumper *Dumper) dumpToFile(file io.Writer) error {
	if dumper.SshDump {
		dumper := NewSshDumper(dumper.SshHost, dumper.SshKey, dumper.SshUser, dumper.ShouldGzip, dumper.DBDriver)
		return dumper.DumpToFile(file)
	} else {
		dumper := NewExecDumper(dumper.ShouldGzip, dumper.DBDriver)
		return dumper.DumpToFile(file)
	}
}

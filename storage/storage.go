package storage

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

type Storage interface {
	Save(reader io.Reader, gzip bool, unique bool) error
}

const cacheDirPrefix = ".onedump"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func generateRandomName(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
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

func EnsureFileName(path string, shouldGzip, unique bool) string {
	p := ensureFileSuffix(path, shouldGzip)
	return ensureUniqueness(p, unique)
}

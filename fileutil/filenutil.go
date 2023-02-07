package fileutil

import (
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Ensure a file has proper file extension.
func EnsureFileSuffix(filename string, shouldGzip bool) string {
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

func EnsureFileName(path string, shouldGzip, unique bool) string {
	p := EnsureFileSuffix(path, shouldGzip)
	return ensureUniqueness(p, unique)
}

func WorkDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Printf("Cannot get the current directory: %v, using $HOME directory!", err)
		dir, err = os.UserHomeDir()
		if err != nil {
			log.Printf("Cannot get the user home directory: %v, using /tmp directory!", err)
			dir = os.TempDir()
		}
	}

	return dir
}

func GenerateRandomName(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

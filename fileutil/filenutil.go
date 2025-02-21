package fileutil

import (
	"bytes"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// Ensure a file has unique name when necessary.
func ensureUniqueness(path string, unique bool) string {
	if !unique {
		return path
	}

	dir, filename := filepath.Split(path)

	now := time.Now().UTC().Format("20060102150405")
	filename = now + "-" + filename

	return filepath.Join(dir, filename)
}

// Ensure a file has proper file extension.
func EnsureFileSuffix(filename string, shouldGzip bool) string {
	if !shouldGzip {
		return filename
	}

	fileExt := filepath.Ext(filename)
	if fileExt == ".gz" {
		return filename
	}

	return filename + ".gz"
}

func EnsureFileName(path string, shouldGzip, unique bool) string {
	p := EnsureFileSuffix(path, shouldGzip)
	return ensureUniqueness(p, unique)
}

func IsGzipped(filename string) bool {
	file, err := os.Open(filename)

	if err != nil {
		return false
	}

	defer func() {
		err := file.Close()
		if err != nil {
			slog.Error("faile to close file", slog.Any("error", err), slog.String("filename", file.Name()))
		}
	}()

	buf := make([]byte, 2)

	_, err = io.ReadFull(file, buf)

	if err != nil {
		return false
	}

	return bytes.Equal(buf, []byte{0x1f, 0x8b})
}

func ListFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string

	for _, v := range entries {
		if v.IsDir() {
			continue
		}

		files = append(files, filepath.Join(dir, v.Name()))
	}

	return files, nil
}

func GenerateRandomName(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
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

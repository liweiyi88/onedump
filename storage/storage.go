package storage

import (
	"fmt"
	"os"
)

type Storage interface {
	CreateDumpFile() (*os.File, error)
}

type CloudStorage interface {
	Upload(filename string) error
}

const uploadDumpCacheDir = ".onedump"

// For uploading dump file to remote storage, we need to firstly dump the db content to a dir locally.
// We use home dir as the cache dir instead of tmp due to the size limit of the tmp dir.
func uploadCacheDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user home dir. %w", err)
	}

	return fmt.Sprintf("%s/%s", homeDir, uploadDumpCacheDir), nil
}

func CreateStorage(filename string) Storage {
	s3Storage, ok := createS3Storage(filename)
	if ok {
		return s3Storage
	}

	return &LocalStorage{
		Filename: filename,
	}
}

package storage

import (
	"fmt"
	"os"
)

type Storage interface {
	CreateDumpFile() (*os.File, error)
}

type CloudStorage interface {
	Upload() error
	CloudFilePath() string
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

func CreateStorage(filename string) (Storage, error) {
	s3Storage, ok, err := createS3Storage(filename)
	if err != nil {
		return nil, err
	}

	if ok {
		return s3Storage, nil
	}

	return &LocalStorage{
		Filename: filename,
	}, nil
}

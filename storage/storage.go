package storage

import (
	"fmt"
	"log"
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
// We firstly try to get current dir, if not successful, then try to get home dir, if still not successful we finally try temp dir
// We need to be aware of the size limit of a temp dir in different OS.
func uploadCacheDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Printf("Cannot get the current directory: %v, using $HOME directory!", err)
		dir, err = os.UserHomeDir()
		if err != nil {
			log.Printf("Cannot get the user home directory: %v, using /tmp directory!", err)
			dir = os.TempDir()
		}
	}

	return fmt.Sprintf("%s/%s", dir, uploadDumpCacheDir)
}

// Factory method to create the storage struct based on filename.
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

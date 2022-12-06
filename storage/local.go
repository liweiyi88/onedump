package storage

import (
	"fmt"
	"os"
)

type LocalStorage struct {
	Filename string
}

func (local *LocalStorage) CreateDumpFile() (*os.File, error) {
	file, err := os.Create(local.Filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create dump file")
	}

	return file, err
}

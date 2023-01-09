package local

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/liweiyi88/onedump/storage"
)

type Local struct {
	Path string `yaml:"path"`
}

func (local *Local) Save(reader io.Reader, gzip bool, unique bool) error {
	path := storage.EnsureFileName(local.Path, gzip, unique)
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create local dump file: %w", err)
	}

	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close local dump file %v", err)
		}
	}()

	_, err = io.Copy(file, reader)

	if err != nil {
		return fmt.Errorf("failed to copy cache file to the dest file: %w", err)
	}

	return nil
}

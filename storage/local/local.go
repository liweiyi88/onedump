package local

import (
	"fmt"
	"io"
	"os"

	"github.com/liweiyi88/onedump/storage"
)

type Local struct {
	Path string `yaml:"path"`
}

func (local *Local) Save(reader io.Reader, gzip bool) error {
	path := storage.EnsureFileSuffix(local.Path, gzip)
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create local dump file: %w", err)
	}

	defer file.Close()

	_, err = io.Copy(file, reader)

	if err != nil {
		return fmt.Errorf("failed to copy cache file to the dest file: %w", err)
	}

	return nil
}

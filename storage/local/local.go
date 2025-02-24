package local

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/liweiyi88/onedump/storage"
)

type Local struct {
	Path string `yaml:"path"`
}

func (local *Local) Save(reader io.Reader, pathGenerator storage.PathGeneratorFunc) error {
	path := pathGenerator(local.Path)

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create local dump file: %w", err)
	}

	defer func() {
		err := file.Close()
		if err != nil {
			slog.Error("fail to close local dump file", slog.Any("error", err))
		}
	}()

	_, err = io.Copy(file, reader)

	if err != nil {
		return fmt.Errorf("failed to copy cache file to the dest file: %w", err)
	}

	return nil
}

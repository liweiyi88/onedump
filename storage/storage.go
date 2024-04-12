package storage

import (
	"io"

	"github.com/liweiyi88/onedump/fileutil"
)

type PathGeneratorFunc func(filename string) string

type Storage interface {
	Save(reader io.Reader, pathGenerator PathGeneratorFunc) error
}

func PathGenerator(gzip bool, unique bool) PathGeneratorFunc {
	return func(filename string) string {
		return fileutil.EnsureFileName(filename, gzip, unique)
	}
}

package runner

import "io"

type Runner interface {
	DumpToFile(file io.Writer) error
}

package dumper

import "io"

type Dumper interface {
	DumpToFile(file io.Writer) error
}

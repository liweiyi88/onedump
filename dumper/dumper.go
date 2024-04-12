package dumper

import "io"

type Dumper interface {
	DumpTo(storage io.Writer) error
}

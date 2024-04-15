package dumper

import "io"

type Dumper interface {
	// Dump db content to storage.
	Dump(storage io.Writer) error
}

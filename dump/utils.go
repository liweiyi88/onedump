package dump

import (
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// Ensure it has proper file suffix if gzip is enabled.
func ensureDumpFileName(dumpFile string, gzip bool) string {
	if !gzip {
		return dumpFile
	}

	if strings.HasSuffix(dumpFile, ".gz") {
		return dumpFile
	}

	return dumpFile + ".gz"
}

func trace(name string) func() {
	start := time.Now()

	return func() {
		elapsed := time.Since(start)
		log.Printf("%s took %s", name, elapsed)
	}
}

func dumpWriters(dumpFile string, shouldGzip bool) (*os.File, *gzip.Writer, error) {
	destDumpFile := ensureDumpFileName(dumpFile, shouldGzip)
	file, err := os.Create(destDumpFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create dump file %w", err)
	}

	gzipWriter := gzip.NewWriter(file)

	return file, gzipWriter, nil
}

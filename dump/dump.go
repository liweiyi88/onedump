package dump

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type CopyDump func(stdout io.Reader) error
type PersistDumpFile func() error

func dump(dumpFile string, shouldGzip bool) (CopyDump, PersistDumpFile, error) {
	destDumpFile := ensureDumpFileName(dumpFile, shouldGzip)
	file, err := os.Create(destDumpFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create dump file %w", err)
	}

	var gzipWriter *gzip.Writer
	if shouldGzip {
		gzipWriter = gzip.NewWriter(file)
	}

	copyDump := func(stdout io.Reader) error {
		if shouldGzip {
			_, err := io.Copy(gzipWriter, stdout)
			if err != nil {
				return err
			}
		} else {
			_, err := io.Copy(file, stdout)
			if err != nil {
				return err
			}
		}

		return nil
	}

	persistDumpFile := func() error {
		if gzipWriter != nil {
			gzipWriter.Close()
		}

		err := file.Close()
		if err != nil {
			return err
		}

		log.Printf("file has been successfully dumped to %s", file.Name())
		// Upload file to s3
		// awssession := awssession.Must(awssession.NewSession())
		// uploader := s3manager.NewUploader(awssession)

		// bucket := "visualgravity"

		// _, err = uploader.Upload(&s3manager.UploadInput{
		// 	Bucket: aws.String(bucket),
		// 	Key:    aws.String("/db_backup/bne.sql.gz"),
		// 	Body:   file,
		// })

		// if err != nil {
		// 	return fmt.Errorf("failed to upload file to s3 bucket %w", err)
		// }

		return nil
	}

	return copyDump, persistDumpFile, nil
}

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

	// if it is not gzip, we should not return the gzipWriter to avoid unnecessary line that contains "<0x00><0x00>..."
	if !shouldGzip {
		return file, nil, nil
	}

	gzipWriter := gzip.NewWriter(file)

	return file, gzipWriter, nil
}

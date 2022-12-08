package dump

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/liweiyi88/onedump/storage"
	"golang.org/x/crypto/ssh"
)

// The core function that dump db content to a file (locally or remotely).
// It checks the filename to determine if we need to upload the file to remote storage or keep it locally.
// For uploading file to S3 bucket, the filename shold follow the pattern: s3://<bucket_name>/<key> .
// For any remote upload, we try to cache it in a local dir then upload it to the remote storage.
func dump(runner any, dumpFile string, shouldGzip bool, command string) error {
	dumpFilename := ensureFileSuffix(dumpFile, shouldGzip)
	store, err := storage.CreateStorage(dumpFilename)
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	file, err := store.CreateDumpFile()
	if err != nil {
		return fmt.Errorf("failed to create storage dump file: %w", err)
	}

	var gzipWriter *gzip.Writer
	if shouldGzip {
		gzipWriter = gzip.NewWriter(file)
	}

	switch runner := runner.(type) {
	case *exec.Cmd:
		runner.Stderr = os.Stderr
		if gzipWriter != nil {
			runner.Stdout = gzipWriter
		} else {
			runner.Stdout = file
		}

		if err := runner.Run(); err != nil {
			return fmt.Errorf("remote command error: %v", err)
		}
	case *ssh.Session:
		var remoteErr bytes.Buffer
		runner.Stderr = &remoteErr
		if gzipWriter != nil {
			runner.Stdout = gzipWriter
		} else {
			runner.Stdout = file
		}

		if err := runner.Run(command); err != nil {
			return fmt.Errorf("remote command error: %s, %v", remoteErr.String(), err)
		}
	default:
		return errors.New("unsupport runner type")
	}

	// Do not pu the below code into a defer function.
	// We need to close them before our cloud storage upload the file.
	// Otherwise we will get corrupted gz file in s3 and we won't be able to expand it.
	if gzipWriter != nil {
		gzipWriter.Close()
	}

	file.Close()

	cloudStore, ok := store.(storage.CloudStorage)

	if ok {
		err := cloudStore.Upload()
		if err != nil {
			return fmt.Errorf("failed to upload file to cloud storage: %w", err)
		}
	}

	return nil
}

// Ensure a file has proper file extension.
func ensureFileSuffix(filename string, shouldGzip bool) string {
	if !shouldGzip {
		return filename
	}

	if strings.HasSuffix(filename, ".gz") {
		return filename
	}

	return filename + ".gz"
}

// Performanece debug function.
func trace(name string) func() {
	start := time.Now()

	return func() {
		elapsed := time.Since(start)
		log.Printf("%s took %s", name, elapsed)
	}
}

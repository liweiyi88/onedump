package dump

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/liweiyi88/onedump/storage"
	"golang.org/x/crypto/ssh"
)

type CopyDump func(stdout io.Reader) error
type PersistDumpFile func() error

// The core function that dump db content to a file (locally or remotely).
// It checks the filename to determin if we need to upload the file to remote storage or we keep it locally.
// For uploading file to S3 bucket, the filename shold follow the pattern: s3://<bucket_name>/<key>
// For any remote uploadk, we tried to cache it in user home dir instead of tmp dir because there is size limit for tmp dir.
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

	defer func() {
		if gzipWriter != nil {
			gzipWriter.Close()
		}

		file.Close()
	}()

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

	cloudStore, ok := store.(storage.CloudStorage)

	if ok {
		err := cloudStore.Upload()
		if err != nil {
			return fmt.Errorf("failed to upload file to cloud storage: %w", err)
		}

		log.Printf("successfully upload dump file to %s", cloudStore.CloudFilePath())
	} else {
		log.Printf("successfully dump file to %s", file.Name())
	}

	return nil
}

// Ensure a file has proper file extension
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

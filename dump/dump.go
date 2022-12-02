package dump

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3BucketInfo struct {
	bucket, key, filename string
}

type CopyDump func(stdout io.Reader) error
type PersistDumpFile func() error

const s3Prefix = "s3://"

func createDumpFile(filename string, remoteDump bool) (*os.File, error) {
	if remoteDump {
		// Use home dir instead of temp dir because temp dir usually has size limit on different OS
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get user home dir")
		}

		file, err := os.Create(homeDir + "/" + filename)
		if err != nil {
			return nil, fmt.Errorf("failed to create dump file in temp dir %w", err)
		}

		return file, err
	}

	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create dump file")
	}

	return file, nil
}

func dump(dumpFile string, shouldGzip bool) (CopyDump, PersistDumpFile, error) {
	dumpFilename := ensureFileSuffix(dumpFile, shouldGzip)
	s3BucketInfo, isS3Dump := extractS3BucketInfo(dumpFilename)

	var file *os.File
	var err error

	if isS3Dump {
		file, err = createDumpFile(s3BucketInfo.filename, true)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create dump file for s3 upload %w", err)
		}
	} else {
		file, err = createDumpFile(dumpFilename, false)
		if err != nil {
			return nil, nil, err
		}
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
		// Remove file on local machie after uploading to s3 bucket.
		defer os.Remove(file.Name())

		if gzipWriter != nil {
			gzipWriter.Close()
		}

		file.Close()

		if isS3Dump {
			uploadFile, err := os.Open(file.Name())
			if err != nil {
				return fmt.Errorf("failed to open dumped file %w", err)
			}

			session := session.Must(session.NewSession())
			uploader := s3manager.NewUploader(session)

			// TODO: implement re-try
			_, err = uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(s3BucketInfo.bucket),
				Key:    aws.String(s3BucketInfo.key),
				Body:   uploadFile,
			})

			if err != nil {
				return fmt.Errorf("failed to upload file to s3 bucket %w", err)
			}

			log.Printf("file has been successfully uploaded to s3: %s", s3BucketInfo.bucket+"/"+s3BucketInfo.key)
		} else {
			log.Printf("file has been successfully dumped to %s", file.Name())
		}
		return nil
	}

	return copyDump, persistDumpFile, nil
}

func ensureFileSuffix(filename string, shouldGzip bool) string {
	if !shouldGzip {
		return filename
	}

	if strings.HasSuffix(filename, ".gz") {
		return filename
	}

	return filename + ".gz"
}

func extractS3BucketInfo(filename string) (*s3BucketInfo, bool) {
	name := strings.TrimSpace(filename)

	if !strings.HasPrefix(name, s3Prefix) {
		return nil, false
	}

	path := strings.TrimPrefix(name, s3Prefix)

	pathChunks := strings.Split(path, "/")
	bucket := pathChunks[0]
	s3Filename := pathChunks[len(pathChunks)-1]
	key := strings.Join(pathChunks[1:], "/")

	return &s3BucketInfo{
		bucket:   bucket,
		key:      key,
		filename: s3Filename,
	}, true
}

func trace(name string) func() {
	start := time.Now()

	return func() {
		elapsed := time.Since(start)
		log.Printf("%s took %s", name, elapsed)
	}
}

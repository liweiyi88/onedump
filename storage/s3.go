package storage

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const s3Prefix = "s3://"

func createS3Storage(filename string) (*S3Storage, bool, error) {
	name := strings.TrimSpace(filename)

	if !strings.HasPrefix(name, s3Prefix) {
		return nil, false, nil
	}

	path := strings.TrimPrefix(name, s3Prefix)

	pathChunks := strings.Split(path, "/")
	bucket := pathChunks[0]
	s3Filename := pathChunks[len(pathChunks)-1]
	key := strings.Join(pathChunks[1:], "/")

	cacheDir, err := uploadCacheDir()
	if err != nil {
		return nil, false, err
	}

	return &S3Storage{
		CacheDir:      cacheDir,
		CacheFile:     s3Filename,
		CacheFilePath: fmt.Sprintf("%s/%s", cacheDir, s3Filename),
		Bucket:        bucket,
		Key:           key,
	}, true, nil
}

type S3Storage struct {
	Bucket        string
	Key           string
	CacheFile     string
	CacheDir      string
	CacheFilePath string
}

func (s3 *S3Storage) CreateDumpFile() (*os.File, error) {
	err := os.MkdirAll(s3.CacheDir, 0750)
	if err != nil {
		return nil, fmt.Errorf("failed to create upload cache dir for remote upload. %w", err)
	}

	file, err := os.Create(s3.CacheFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create dump file in cache dir. %w", err)
	}

	return file, err
}

func (s3 *S3Storage) Upload() error {
	uploadFile, err := os.Open(s3.CacheFilePath)
	if err != nil {
		return fmt.Errorf("failed to open dumped file %w", err)
	}

	defer uploadFile.Close()

	session := session.Must(session.NewSession())
	uploader := s3manager.NewUploader(session)

	log.Printf("uploading file %s to s3...", uploadFile.Name())
	// TODO: implement re-try
	_, uploadErr := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3.Bucket),
		Key:    aws.String(s3.Key),
		Body:   uploadFile,
	})

	// Remove local cache dir after uploading to s3 bucket.
	log.Printf("removing cache dir %s ... ", s3.CacheDir)
	err = os.RemoveAll(s3.CacheDir)
	if err != nil {
		log.Println("failed to remove cache dir after uploading to s3", err)
	}

	if uploadErr != nil {
		return fmt.Errorf("failed to upload file to s3 bucket %w", uploadErr)
	}

	return nil
}

func (s3 *S3Storage) CloudFilePath() string {
	return fmt.Sprintf("s3://%s/%s", s3.Bucket, s3.Key)
}

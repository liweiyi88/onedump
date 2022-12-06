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

func createS3Storage(filename string) (*S3Storage, bool) {
	name := strings.TrimSpace(filename)

	if !strings.HasPrefix(name, s3Prefix) {
		return nil, false
	}

	path := strings.TrimPrefix(name, s3Prefix)

	pathChunks := strings.Split(path, "/")
	bucket := pathChunks[0]
	s3Filename := pathChunks[len(pathChunks)-1]
	key := strings.Join(pathChunks[1:], "/")

	return &S3Storage{
		CacheFile: filename,
		Bucket:    bucket,
		Key:       key,
		Filename:  s3Filename,
	}, true
}

type S3Storage struct {
	Bucket    string
	Key       string
	Filename  string
	CacheFile string
}

func (s3 *S3Storage) CreateDumpFile() (*os.File, error) {
	cacheDir, err := uploadCacheDir()
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(cacheDir, 0750)
	if err != nil {
		return nil, fmt.Errorf("failed to create upload cache dir for remote upload. %w", err)
	}

	file, err := os.Create(fmt.Sprintf("%s/%s", cacheDir, s3.CacheFile))
	if err != nil {
		return nil, fmt.Errorf("failed to create dump file in cache dir. %w", err)
	}

	return file, err
}

func (s3 *S3Storage) Upload(filename string) error {
	uploadFile, err := os.Open(filename)
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

	// Remove file on local machie after uploading to s3 bucket.
	cacheDir, err := uploadCacheDir()
	if err != nil {
		log.Println("failed to get cache dir after uploading to s3", err)
	}

	log.Printf("removing cache dir %s ... ", cacheDir)
	err = os.RemoveAll(cacheDir)
	if err != nil {
		log.Println("failed to remove cache dir after uploading to s3", err)
	}

	if uploadErr != nil {
		return fmt.Errorf("failed to upload file to s3 bucket %w", uploadErr)
	}

	log.Printf("file has been successfully uploaded to s3: %s", s3.Bucket+"/"+s3.Key)

	return nil
}

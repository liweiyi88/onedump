package storage

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const S3Prefix = "s3://"

var ErrInvalidS3Path = fmt.Errorf("invalid s3 filename, it should follow the format %s<bucket>/<path|filename>", S3Prefix)

func NewS3Storage(bucket, key string, credentials *AWSCredentials) *S3Storage {
	return &S3Storage{
		Credentials: credentials,
		Bucket:      bucket,
		Key:         key,
	}
}

type AWSCredentials struct {
	Region          string `yaml:"region"`
	AccessKeyId     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
}

type S3Storage struct {
	Bucket      string
	Key         string
	Credentials *AWSCredentials
}

func (s3 *S3Storage) Upload(reader io.ReadCloser) error {
	defer func() {
		reader.Close()
	}()

	var awsConfig aws.Config
	if s3.Credentials != nil {
		if s3.Credentials.Region != "" {
			awsConfig.Region = aws.String(s3.Credentials.Region)
		}

		if s3.Credentials.AccessKeyId != "" && s3.Credentials.SecretAccessKey != "" {
			awsConfig.Credentials = credentials.NewStaticCredentials(s3.Credentials.AccessKeyId, s3.Credentials.SecretAccessKey, "")
		}
	}

	session := session.Must(session.NewSession(&awsConfig))
	uploader := s3manager.NewUploader(session)

	// TODO: implement re-try
	_, uploadErr := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3.Bucket),
		Key:    aws.String(s3.Key),
		Body:   reader,
	})

	if uploadErr != nil {
		return fmt.Errorf("failed to upload file to s3 bucket %w", uploadErr)
	}

	return nil
}

func (s3 *S3Storage) CloudFilePath() string {
	return fmt.Sprintf("s3://%s/%s", s3.Bucket, s3.Key)
}

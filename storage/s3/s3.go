package s3

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/liweiyi88/onedump/storage"
)

func NewS3(bucket, key, region, accessKeyId, secretAccessKey string) *S3 {
	return &S3{
		Bucket:          bucket,
		Key:             key,
		Region:          region,
		AccessKeyId:     accessKeyId,
		SecretAccessKey: secretAccessKey,
	}
}

type S3 struct {
	Bucket          string
	Key             string
	Region          string `yaml:"region"`
	AccessKeyId     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
}

func (s3 *S3) Save(reader io.Reader, gzip bool) error {
	var awsConfig aws.Config

	if s3.Region != "" {
		awsConfig.Region = aws.String(s3.Region)
	}

	if s3.AccessKeyId != "" && s3.SecretAccessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(s3.AccessKeyId, s3.SecretAccessKey, "")
	}

	session := session.Must(session.NewSession(&awsConfig))
	uploader := s3manager.NewUploader(session)

	key := storage.EnsureFileSuffix(s3.Key, gzip)

	// TODO: implement re-try
	_, uploadErr := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	})

	if uploadErr != nil {
		return fmt.Errorf("failed to upload file to s3 bucket %w", uploadErr)
	}

	return nil
}

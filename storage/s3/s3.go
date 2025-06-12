package s3

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3Client "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/liweiyi88/onedump/storage"
)

func NewS3(bucket, key, region, accessKeyId, secretAccessKey, sessionToken string) *S3 {
	s3 := &S3{
		Bucket:          bucket,
		Key:             key,
		Region:          region,
		AccessKeyId:     accessKeyId,
		SecretAccessKey: secretAccessKey,
		SessionToken:    sessionToken,
	}

	return s3
}

type S3 struct {
	Bucket          string
	Key             string
	Region          string `yaml:"region"`
	AccessKeyId     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
	SessionToken    string `yaml:"session-token"`
	client          *s3Client.Client
}

func (s3 *S3) createClient() *s3Client.Client {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(s3.Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(s3.AccessKeyId, s3.SecretAccessKey, s3.SessionToken),
		))

	if err != nil {
		panic(fmt.Sprintf("[s3] failed to load config, %v", err))
	}

	return s3Client.NewFromConfig(cfg)
}

// Download a S3 object content to a local file using streaming
func (s3 *S3) downloadObjectToDir(ctx context.Context, prefix, key, dir string) error {
	client := s3.getClient()

	slog.Debug("[s3] downloading content...", slog.Any("key", key))

	result, err := client.GetObject(ctx, &s3Client.GetObjectInput{
		Bucket: &s3.Bucket,
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("%v unable to get s3 content", err)
	}

	defer func() {
		if err := result.Body.Close(); err != nil {
			slog.Error("[s3] fail to close result body", slog.Any("action", "DownloadContent"), slog.Any("error", err))
		}
	}()

	path := strings.TrimPrefix(key, prefix)
	path = strings.TrimLeft(path, "/")

	localPath := filepath.Join(dir, path)
	err = os.MkdirAll(filepath.Dir(localPath), os.ModePerm)

	if err != nil {
		return fmt.Errorf("[s3] fail to create local folders error: %v", err)
	}

	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("[s3] fail to create local file for the download content, error: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("[s3] fail to close file", slog.Any("filename", file.Name()), slog.Any("error", err))
		}
	}()

	_, err = io.Copy(file, result.Body)
	return err
}

func (s3 *S3) getClient() *s3Client.Client {
	if s3.client == nil {
		s3.client = s3.createClient()
		return s3.client
	}

	credentials, err := s3.client.Options().Credentials.Retrieve(context.Background())

	if err != nil || credentials.Expired() {
		s3.client = s3.createClient()
		return s3.client
	}

	return s3.client
}

func (s3 *S3) Save(reader io.Reader, pathGenerator storage.PathGeneratorFunc) error {
	uploader := manager.NewUploader(s3.getClient())

	key := pathGenerator(s3.Key)

	slog.Debug("[s3] start to upload file to s3 bucket", slog.Any("bucket", s3.Bucket), slog.Any("key", key))

	_, uploadErr := uploader.Upload(context.Background(), &s3Client.PutObjectInput{
		Bucket: aws.String(s3.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	})

	if uploadErr != nil {
		return fmt.Errorf("fail to upload file to S3 bucket: %s, key: %s, error: %w", s3.Bucket, key, uploadErr)
	}

	slog.Debug("[s3] the file has been uploaded to the S3 bucket", slog.Any("bucket", s3.Bucket), slog.Any("key", key))

	return nil
}

func (s3 *S3) DownloadObjects(ctx context.Context, prefix, dir string) error {
	client := s3.getClient()

	paginator := s3Client.NewListObjectsV2Paginator(client, &s3Client.ListObjectsV2Input{
		Bucket: aws.String(s3.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)

		if err != nil {
			return fmt.Errorf("[s3] fail to get next page while downloading, error: %v", err)
		}

		for _, object := range page.Contents {
			key := aws.ToString(object.Key)

			if key == "" || strings.HasSuffix(key, "/") {
				// skip empty keys and directory placeholders
				continue
			}

			err := s3.downloadObjectToDir(ctx, prefix, key, dir)

			if err != nil {
				return fmt.Errorf("[s3] fail to download content, file key: %s, error: %v", key, err)
			}
		}
	}

	return nil
}

// Read full S3 object content into memory
func (s3 *S3) GetContent(ctx context.Context) ([]byte, error) {
	client := s3.getClient()

	result, err := client.GetObject(ctx, &s3Client.GetObjectInput{
		Bucket: &s3.Bucket,
		Key:    &s3.Key,
	})

	if err != nil {
		return nil, fmt.Errorf("%v unable to get s3 content", err)
	}

	defer func() {
		if err := result.Body.Close(); err != nil {
			slog.Error("[s3] fail to close result body", slog.Any("action", "GetContent"), slog.Any("error", err))
		}
	}()

	return io.ReadAll(result.Body)
}

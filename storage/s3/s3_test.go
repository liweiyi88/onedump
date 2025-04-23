package s3

import (
	"errors"
	"strings"
	"testing"

	"github.com/liweiyi88/onedump/storage"
	"github.com/stretchr/testify/assert"
)

func TestNewS3(t *testing.T) {
	assert := assert.New(t)
	expectedBucket := "onedump"
	expectedKey := "/backup/dump.sql"
	expectedRegion := "ap-southeast-2"
	expectedAccessKeyId := "accessKey"
	expectedSecretKey := "secret"

	s3 := NewS3(expectedBucket, expectedKey, expectedRegion, expectedAccessKeyId, expectedSecretKey, "")

	assert.Equal(expectedBucket, s3.Bucket)
	assert.Equal(expectedKey, s3.Key)
	assert.Equal(expectedRegion, s3.Region)
	assert.Equal(expectedAccessKeyId, s3.AccessKeyId)
	assert.Equal(expectedSecretKey, s3.SecretAccessKey)
}

func TestSave(t *testing.T) {
	s3 := &S3{
		Bucket:          "onedump",
		Key:             "/backup/dump.sql",
		Region:          "ap-southeast-2",
		AccessKeyId:     "none",
		SecretAccessKey: "none",
	}

	reader := strings.NewReader("hello s3")
	err := s3.Save(reader, storage.PathGenerator(true, true))

	actual := errors.Unwrap(err).Error()
	assert.True(t, strings.HasPrefix(actual, "InvalidAccessKeyId"))
}

func TestGetContent(t *testing.T) {
	s3 := &S3{
		Bucket:          "onedump",
		Key:             "/backup/dump.sql",
		Region:          "ap-southeast-2",
		AccessKeyId:     "none",
		SecretAccessKey: "none",
	}

	_, err := s3.GetContent()

	assert.True(t, strings.HasPrefix(err.Error(), "InvalidAccessKeyId"))
}

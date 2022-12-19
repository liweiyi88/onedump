package s3

import (
	"errors"
	"strings"
	"testing"
)

func TestNewS3(t *testing.T) {
	expectedBucket := "onedump"
	expectedKey := "/backup/dump.sql"
	expectedRegion := "ap-southeast-2"
	expectedAccessKeyId := "accessKey"
	expectedSecretKey := "secret"

	s3 := NewS3(expectedBucket, expectedKey, expectedRegion, expectedAccessKeyId, expectedSecretKey)

	if s3.Bucket != expectedBucket {
		t.Errorf("expected: %s, actual: %s", expectedBucket, s3.Bucket)
	}

	if s3.Key != expectedKey {
		t.Errorf("expected: %s, actual: %s", expectedBucket, s3.Key)
	}

	if s3.Region != expectedRegion {
		t.Errorf("expected: %s, actual: %s", expectedRegion, s3.Region)
	}

	if s3.AccessKeyId != expectedAccessKeyId {
		t.Errorf("expected: %s, actual: %s", expectedAccessKeyId, s3.AccessKeyId)
	}

	if s3.SecretAccessKey != expectedSecretKey {
		t.Errorf("expected: %s, actual: %s", expectedSecretKey, s3.SecretAccessKey)
	}
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
	err := s3.Save(reader, true)

	actual := errors.Unwrap(err).Error()
	if !strings.HasPrefix(actual, "InvalidAccessKeyId") {
		t.Errorf("expeceted invalid access key id but actual got: %s", actual)
	}
}

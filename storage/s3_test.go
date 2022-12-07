package storage

import (
	"errors"
	"testing"
)

func TestCreateS3Storage(t *testing.T) {
	store, ok, err := createS3Storage("://")
	if ok || store != nil || err != nil {
		t.Error("it should create no store, returns not ok without err")
	}

	store, ok, err = createS3Storage("s3://fdfdf")
	if ok || store != nil || !errors.Is(err, ErrInvalidS3Path) {
		t.Error("it is an invalid s3 filename, it should create no store, returns not ok with ErrInvalidS3Path")
	}

	store, ok, err = createS3Storage("s3://bucket/path/to/file.jpg")

	if !ok || err != nil {
		t.Error("expected it should create a s3 storage", err)
	}

	if store.Bucket != "bucket" || store.Key != "path/to/file.jpg" || store.CacheFile != "file.jpg" || store.CacheDir != uploadCacheDir() {
		t.Errorf("store has unexpected fields: %+v", store)
	}
}

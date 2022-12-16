package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestUploadCacheDir(t *testing.T) {
	actual := UploadCacheDir()

	workDir, _ := os.Getwd()
	expected := fmt.Sprintf("%s/%s", workDir, uploadDumpCacheDir)

	if actual != expected {
		t.Errorf("get unexpected cache dir: expected: %s, actual: %s", expected, actual)
	}
}

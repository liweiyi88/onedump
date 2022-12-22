package storage

import (
	"fmt"
	"os"
	"strings"
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

func TestGenerateCacheFileName(t *testing.T) {
	expectedLen := 5
	name := generateCacheFileName(expectedLen)

	actualLen := len([]rune(name))
	if actualLen != expectedLen {
		t.Errorf("unexpected cache filename, expected length: %d, actual length: %d", 5, actualLen)
	}
}

func TestUploadCacheFilePath(t *testing.T) {
	gziped := UploadCacheFilePath(true)

	if !strings.HasSuffix(gziped, ".gz") {
		t.Errorf("expected filename has .gz extention, actual file name: %s", gziped)
	}

	sql := UploadCacheFilePath(false)

	if !strings.HasSuffix(sql, ".sql") {
		t.Errorf("expected filename has .sql extention, actual file name: %s", sql)
	}

	sql2 := UploadCacheFilePath(false)

	if sql == sql2 {
		t.Errorf("expected unique file name but got same filename %s", sql)
	}
}

func TestEnsureFileSuffix(t *testing.T) {
	gzip := EnsureFileSuffix("test.sql", true)
	if gzip != "test.sql.gz" {
		t.Errorf("expected filename has .gz extention, actual file name: %s", gzip)
	}

	sql := EnsureFileSuffix("test.sql.gz", true)

	if sql != "test.sql.gz" {
		t.Errorf("expected: %s is not equals to actual: %s", sql, "test.sql.gz")
	}
}

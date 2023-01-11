package storage

import (
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func TestUploadCacheDir(t *testing.T) {
	actual := cacheFileDir()

	workDir, _ := os.Getwd()
	prefix := fmt.Sprintf("%s/%s", workDir, cacheDirPrefix)

	if !strings.HasPrefix(actual, prefix) {
		t.Errorf("get unexpected cache dir: expected: %s, actual: %s", prefix, actual)
	}
}

func TestGenerateCacheFileName(t *testing.T) {
	expectedLen := 5
	name := generateRandomName(expectedLen)

	actualLen := len([]rune(name))
	if actualLen != expectedLen {
		t.Errorf("unexpected cache filename, expected length: %d, actual length: %d", 5, actualLen)
	}
}

func TestUploadCacheFilePath(t *testing.T) {

	cacheDir := cacheFileDir()

	gziped := cacheFilePath(cacheDir, true)

	if !strings.HasSuffix(gziped, ".gz") {
		t.Errorf("expected filename has .gz extention, actual file name: %s", gziped)
	}

	sql := cacheFilePath(cacheDir, false)

	if !strings.HasSuffix(sql, ".sql") {
		t.Errorf("expected filename has .sql extention, actual file name: %s", sql)
	}

	sql2 := cacheFilePath(cacheDir, false)

	if sql == sql2 {
		t.Errorf("expected unique file name but got same filename %s", sql)
	}
}

func TestEnsureFileSuffix(t *testing.T) {
	gzip := ensureFileSuffix("test.sql", true)
	if gzip != "test.sql.gz" {
		t.Errorf("expected filename has .gz extention, actual file name: %s", gzip)
	}

	sql := ensureFileSuffix("test.sql.gz", true)

	if sql != "test.sql.gz" {
		t.Errorf("expected: %s is not equals to actual: %s", sql, "test.sql.gz")
	}
}

func TestEnsureUniqueness(t *testing.T) {
	path := "/Users/jack/Desktop/hello.sql"

	p := ensureUniqueness("/Users/jack/Desktop/hello.sql", false)
	if path != p {
		t.Errorf("expected same paths but got %s", p)
	}

	p = ensureUniqueness("/Users/jack/Desktop/hello.sql", true)

	if !strings.HasPrefix(p, "/Users/jack/Desktop/") {
		t.Errorf("got incorrect path: %s", p)
	}

	s := strings.Split(p, "/")
	filename := s[len(s)-1]

	now := time.Now().UTC().Format("2006010215")

	if !strings.HasPrefix(filename, now) {
		t.Errorf("got incorrect filename prefix: %s", filename)
	}

	if !strings.HasSuffix(filename, "-hello.sql") {
		t.Errorf("got incorrect filename suffix: %s", filename)
	}
}

func TestCreateCacheFile(t *testing.T) {
	file, cacheDir, _ := CreateCacheFile(true)

	defer func() {
		file.Close()

		err := os.RemoveAll(cacheDir)
		if err != nil {
			log.Println("failed to remove cache dir after dump", err)
		}
	}()

	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		t.Errorf("failed to get cache file info %v", err)
	}

	if fileInfo.Size() != 0 {
		t.Errorf("expected empty file but get size: %d", fileInfo.Size())
	}
}

func TestEnsureFileName(t *testing.T) {
	p := EnsureFileName("/Users/jack/Desktop/hello.sql", true, false)

	if p != "/Users/jack/Desktop/hello.sql.gz" {
		t.Errorf("unexpected filename: %s", p)
	}
}

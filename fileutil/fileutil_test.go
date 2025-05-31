package fileutil

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnsureFileName(t *testing.T) {
	p := EnsureFileName("/Users/jack/Desktop/hello.sql", true, false)
	assert.Equal(t, "/Users/jack/Desktop/hello.sql.gz", p)
}

func TestEnsureFileSuffix(t *testing.T) {
	assert := assert.New(t)
	f := EnsureFileSuffix("test.sql", true)
	assert.Equal("test.sql.gz", f)

	f = EnsureFileSuffix("test.sql.gz", true)
	assert.Equal("test.sql.gz", f)

	f = EnsureFileSuffix("test.sql", false)
	assert.Equal("test.sql", f)
}

func TestEnsureUniqueness(t *testing.T) {
	assert := assert.New(t)
	path := "/Users/jack/Desktop/hello.sql"

	p := ensureUniqueness(path, false)
	assert.Equal(p, path)

	p = ensureUniqueness(path, true)

	_, filename := filepath.Split(p)

	now := time.Now().UTC().Format("2006010215")

	assert.True(strings.HasPrefix(filename, now))
	assert.True(strings.HasSuffix(filename, "-hello.sql"))
}

func TestGenerateRandomName(t *testing.T) {
	n := GenerateRandomName(10)
	assert.Len(t, n, 10)
}

func TestWorkDir(t *testing.T) {
	dir := WorkDir()
	wd, _ := os.Getwd()
	if dir != wd {
		t.Errorf("Expected %s, got %s", wd, dir)
	}
}

func TestListFiles(t *testing.T) {
	assert := assert.New(t)

	tempDir, err := os.MkdirTemp("", "testdir")
	assert.NoError(err)

	defer os.RemoveAll(tempDir) // Clean up after test

	files := []string{"file1.txt", "file2.log", "file3.txt"}
	for _, f := range files {
		filePath := filepath.Join(tempDir, f)
		err := os.WriteFile(filePath, []byte("test"), 0644)
		assert.NoError(err)
	}

	subDir := filepath.Join(tempDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	assert.NoError(err)

	// Test without pattern (should return all files)
	result, err := ListFiles(tempDir, "", "")
	assert.NoError(err)

	expected := []string{
		filepath.Join(tempDir, "file1.txt"),
		filepath.Join(tempDir, "file2.log"),
		filepath.Join(tempDir, "file3.txt"),
	}

	assert.Equal(result, expected)

	// Test with pattern (*.txt)
	expected = []string{
		filepath.Join(tempDir, "file1.txt"),
		filepath.Join(tempDir, "file3.txt"),
	}
	result, err = ListFiles(tempDir, "*.txt", "")
	assert.NoError(err)
	assert.Equal(result, expected)

	result, err = ListFiles(tempDir, "[invalid]", "")
	assert.NoError(err)
	assert.Len(result, 0)

	// Test with skipExt option
	expected = []string{
		filepath.Join(tempDir, "file1.txt"),
		filepath.Join(tempDir, "file3.txt"),
	}

	result, err = ListFiles(tempDir, "", ".log")
	assert.NoError(err)
	assert.Len(result, 2)
}

func TestIsGzipped(t *testing.T) {
	gzFile, err := os.CreateTemp("", "testfile.gz")

	if err != nil {
		t.Fatalf("Failed to create temp gzip file: %v", err)
	}

	defer gzFile.Close()
	defer os.Remove(gzFile.Name())

	// Write gzip header and some content
	gzWriter := gzip.NewWriter(gzFile)
	_, err = gzWriter.Write([]byte("test data"))

	if err != nil {
		t.Fatalf("Failed to write to gzip file: %v", err)
	}

	gzWriter.Close()

	if !IsGzipped(gzFile.Name()) {
		t.Errorf("Expected true for gzip file, got false")
	}

	txtFile, err := os.CreateTemp("", "testfile.txt")

	if err != nil {
		t.Fatalf("Failed to create temp text file: %v", err)
	}

	defer os.Remove(txtFile.Name())

	_, err = txtFile.Write([]byte("this is a plain text file"))

	if err != nil {
		t.Fatalf("Failed to write to text file: %v", err)
	}

	txtFile.Close()

	if IsGzipped(txtFile.Name()) {
		t.Errorf("Expected false for non-gzip file, got true")
	}

	if IsGzipped("/non/existent/file.gz") {
		t.Errorf("Expected false for non-existent file, got true")
	}
}

package fileutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createTestFile(filename string, content string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	_, err = file.WriteString(content)
	if err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	return nil
}

func TestComputeChecksum(t *testing.T) {
	filename := "test_checksum.txt"

	if err := createTestFile(filename, "test content"); err != nil {
		t.Error(err)
	}

	defer func() {
		if err := os.Remove(filename); err != nil {
			t.Error(err)
		}
	}()

	file, err := os.Open(filename)

	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	f := NewChecksum(file)

	checksum, err := f.computeChecksum()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, checksum, 64)

	filename2 := "another_chcksum.txt"
	if err := createTestFile(filename2, "test content"); err != nil {
		t.Error(err)
	}

	defer func() {
		if err := os.Remove(filename2); err != nil {
			t.Error(err)
		}
	}()

	file2, err := os.Open(filename2)
	if err != nil {
		t.Error(err)
	}

	defer file2.Close()

	f.File = file2
	checksum2, err := f.computeChecksum()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, checksum2, 64)
	assert.Equal(t, checksum2, checksum)
}

package filesync

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

	f := NewChecksum(filename, "")

	checksum, err := f.computeChecksum()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, checksum, 64)

	filename2 := "another_checksum.txt"
	if err := createTestFile(filename2, "test content"); err != nil {
		t.Error(err)
	}

	defer func() {
		if err := os.Remove(filename2); err != nil {
			t.Error(err)
		}
	}()

	f.filePath = filename2
	checksum2, err := f.computeChecksum()
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, checksum2, 64)
	assert.Equal(t, checksum2, checksum)
}

func TestGetStateFilePath(t *testing.T) {
	checksum := NewChecksum("", "")
	assert.Equal(t, "checksum.onedump", checksum.getStateFilePath())
}

func TestIsFileTransferred(t *testing.T) {
	assert := assert.New(t)
	filename := "test_checksum.txt"

	if err := createTestFile(filename, "test content"); err != nil {
		t.Error(err)
	}

	defer func() {
		if err := os.Remove(filename); err != nil {
			t.Error(err)
		}
	}()

	checksum := NewChecksum(filename, "")

	transferred, err := checksum.IsFileTransferred()
	assert.Nil(err)
	assert.False(transferred)

	err = checksum.SaveState()
	assert.Nil(err)

	defer func() {
		if err := checksum.DeleteState(); err != nil {
			t.Error(err)
		}
	}()

	transferred, err = checksum.IsFileTransferred()
	assert.Nil(err)
	assert.True(transferred)
}

func TestDeleteState(t *testing.T) {
	checksum := NewChecksum("", "")
	err := checksum.DeleteState()
	assert.Nil(t, err)
}

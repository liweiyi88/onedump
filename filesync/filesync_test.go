package filesync

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasSynced(t *testing.T) {
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

	fs := NewFileSync(false, "")

	synced, err := fs.HasSynced(filename)
	assert.Nil(err)
	assert.False(synced)
}

func TestSyncFile(t *testing.T) {
	assert := assert.New(t)

	tempDir := t.TempDir()
	file := filepath.Join(tempDir, "sync-file.txt")

	f, err := os.Create(file)
	assert.NoError(err)

	defer func() {
		err := f.Close()
		assert.NoError(err)

		stateFile := filepath.Join(filepath.Dir(file), ChecksumStateFile)
		_ = os.Remove(stateFile)

		err = os.Remove(file)
		assert.NoError(err)
	}()

	fs := NewFileSync(true, "")
	err = fs.SyncFile(file, func() error { return nil })
	assert.NoError(err)

	fs = NewFileSync(false, "")
	err = fs.SyncFile(file, func() error { return nil })
	assert.NoError(err)

	fs = NewFileSync(true, "")
	err = fs.SyncFile(file, func() error { return fmt.Errorf("sync error") })
	assert.Error(err)
	assert.Contains(err.Error(), "sync error")

	customStateFile := filepath.Join(tempDir, "custom-checksum-state.log")
	fs = NewFileSync(true, customStateFile)
	err = fs.SyncFile(file, func() error { return nil })

	defer func() {
		err := os.Remove(customStateFile)
		assert.NoError(err)
	}()

	assert.NoError(err)
	assert.FileExists(customStateFile)
}

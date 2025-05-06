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

	synced, err := HasSynced(filename)
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

	err = SyncFile(file, true, func() error { return nil })
	assert.NoError(err)

	err = SyncFile(file, false, func() error { return nil })
	assert.NoError(err)

	err = SyncFile(file, true, func() error { return fmt.Errorf("sync error") })
	assert.Error(err)
	assert.Contains(err.Error(), "sync error")
}

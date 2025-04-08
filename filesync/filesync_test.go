package filesync

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncFile(t *testing.T) {
	assert := assert.New(t)

	file := "sync-file.txt"
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

	err = SyncFile("sync-file.txt", true, func() error { return nil })
	assert.NoError(err)

	err = SyncFile("sync-file.txt", false, func() error { return nil })
	assert.NoError(err)

	err = SyncFile("sync-file.txt", true, func() error { return fmt.Errorf("sync error") })
	assert.Error(err)
	assert.Contains(err.Error(), "sync error")
}

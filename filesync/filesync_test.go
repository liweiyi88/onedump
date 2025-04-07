package filesync

import (
	"os"
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

		err = os.Remove(file)
		assert.NoError(err)
	}()

	err = SyncFile("sync-file.txt", true, func() error { return nil })
	assert.NoError(err)

	err = SyncFile("sync-file.txt", false, func() error { return nil })
	assert.NoError(err)
}

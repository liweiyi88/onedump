package local

import (
	"os"
	"strings"
	"testing"

	"github.com/liweiyi88/onedump/storage"
	"github.com/stretchr/testify/assert"
)

func TestSave(t *testing.T) {
	filename := os.TempDir() + "/test.sql.gz"
	local := &Local{Path: filename}

	expected := "hello"
	reader := strings.NewReader(expected)

	err := local.Save(reader, storage.PathGenerator(true, false))
	assert.Nil(t, err)

	data, err := os.ReadFile(filename)
	assert.Nil(t, err)

	assert.Equal(t, expected, string(data))
	defer os.Remove(filename)
}

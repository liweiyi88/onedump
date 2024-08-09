package fileutil

import (
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

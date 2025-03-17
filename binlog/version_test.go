package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitServerVersion(t *testing.T) {
	version := "8.0.22-0ubuntu0.20.04.2"

	marjor, minor, patch := splitServerVersion(version)

	assert := assert.New(t)
	assert.Equal(8, marjor)
	assert.Equal(0, minor)
	assert.Equal(22, patch)

	version = "8.4-0ubuntu0.20.04.2"

	marjor, minor, patch = splitServerVersion(version)

	assert.Equal(8, marjor)
	assert.Equal(4, minor)
	assert.Equal(0, patch)
}

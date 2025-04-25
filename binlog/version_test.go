package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitServerVersion(t *testing.T) {
	version := "8.0.22-0ubuntu0.20.04.2"

	mysqlVersion := splitServerVersion(version)

	assert := assert.New(t)
	assert.Equal(8, mysqlVersion.major)
	assert.Equal(0, mysqlVersion.minor)
	assert.Equal(22, mysqlVersion.patch)

	version = "8.4-0ubuntu0.20.04.2"

	mysqlVersion = splitServerVersion(version)

	assert.Equal(8, mysqlVersion.major)
	assert.Equal(4, mysqlVersion.minor)
	assert.Equal(0, mysqlVersion.patch)
}

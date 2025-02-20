package slow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser(t *testing.T) {
	Parse("testutils/slowlog_mysql.log", MySQL)
}

func TestGetParser(t *testing.T) {
	_, err := getParser("unknown")
	assert.Equal(t, "unsupported database type: unknown", err.Error())

	parser, err := getParser(MySQL)
	assert.Nil(t, err)
	assert.Equal(t, parser, NewMySQLSlowLogParser())
}

package runner

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecRunner(t *testing.T) {
	runner := NewExecRunner("echo", []string{"hello"}, []string{"APP_ENV=test"})

	var b bytes.Buffer

	runner.Run(&b)
	assert.Equal(t, "hello\n", b.String())

	runner = NewExecRunner("echo hello", nil, []string{"APP_ENV=test"})
	err := runner.Run(&b)

	assert.NotNil(t, err)
}

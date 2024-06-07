package dialer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnsureSSHHostHavePort(t *testing.T) {
	assert := assert.New(t)
	sshHost := "127.0.0.1"

	assert.Equal(sshHost+":22", ensureHaveSSHPort(sshHost))

	sshHost = "127.0.0.1:22"
	actual := ensureHaveSSHPort(sshHost)
	assert.Equal(sshHost, actual)
}

package dialer

import (
	"testing"

	"github.com/liweiyi88/onedump/testutils"
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

func TestCreateSshClient(t *testing.T) {
	assert := assert.New(t)

	ssh := NewSsh("localhost", "random_key", "root")

	_, err := ssh.CreateSshClient()
	assert.Equal("failed to create ssh singer :ssh: no key found", err.Error())

	privateKey, err := testutils.GenerateRSAPrivateKey()
	assert.Nil(err)

	ssh = NewSsh("localhost", privateKey, "root")

	_, err = ssh.CreateSshClient()
	assert.NotNil(err)
}

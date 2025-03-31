package dialer

import (
	"encoding/base64"
	"os"
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

func TestParseSSHKey(t *testing.T) {
	assert := assert.New(t)

	file, err := os.Create("privateKey.pem")
	assert.Nil(err)

	defer func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}

		if err := os.Remove(file.Name()); err != nil {
			t.Error(err)
		}
	}()

	_, err = parseSSHKey(file.Name())
	assert.Nil(err)

	base64Encoded := base64.StdEncoding.EncodeToString([]byte{'f'})
	key, err := parseSSHKey(base64Encoded)
	assert.Equal("f", string(key))
}

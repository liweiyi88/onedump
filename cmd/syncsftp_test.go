package cmd

import (
	"bytes"
	"os"
	"testing"

	"github.com/liweiyi88/onedump/testutils"
	"github.com/stretchr/testify/assert"
)

func TestSyncSftpCmd(t *testing.T) {
	cmd := rootCmd
	assert := assert.New(t)

	t.Run("it should return err if we try to sync multiple files but specify a single file as destination", func(t *testing.T) {
		currentDir, err := os.Getwd()
		assert.Nil(err)

		privateKey, err := testutils.GenerateRSAPrivateKey()
		assert.Nil(err)

		finishCh := make(chan struct{}, 1)

		onClient := func() {
			cmd.SetArgs([]string{
				"sync", "sftp",
				"--source=" + currentDir, "--destination=" + currentDir + "/dest.txt",
				"--ssh-host=127.0.0.1:20005",
				"--ssh-user=root",
				"--ssh-key=" + privateKey,
			})

			buf := make([]byte, 1024)
			readWriter := bytes.NewBuffer(buf)

			cmd.SetOut(readWriter)
			cmd.SetErr(readWriter)
			cmd.Execute()

			assert.Contains(readWriter.String(), "Error: detination should not be a file when transfer multiple files from the source")
			finishCh <- struct{}{}
		}

		testutils.StartSftpServer("0.0.0.0:20005", privateKey, 1, onClient)
		<-finishCh
	})

	t.Run("it should transfer a single file from source to destination", func(t *testing.T) {
		source, err := os.Create("single_source.txt")
		assert.Nil(err)

		defer func() {
			source.Close()
			os.Remove("single_source.txt")
		}()

		source.WriteString("source content")

		currentDir, err := os.Getwd()
		assert.Nil(err)

		privateKey, err := testutils.GenerateRSAPrivateKey()
		assert.Nil(err)

		finishCh := make(chan struct{}, 1)

		onClient := func() {
			destPath := currentDir + "/dest.txt"
			defer os.Remove(destPath)

			cmd.SetArgs([]string{
				"sync", "sftp",
				"--source=" + source.Name(), "--destination=" + destPath,
				"--ssh-host=127.0.0.1:20005",
				"--ssh-user=root",
				"--ssh-key=" + privateKey,
				"-v",
			})

			buf := make([]byte, 1024)
			readWriter := bytes.NewBuffer(buf)

			cmd.SetOut(readWriter)
			cmd.SetErr(readWriter)
			cmd.Execute()

			assert.Contains(readWriter.String(), "")
			finishCh <- struct{}{}
		}

		testutils.StartSftpServer("0.0.0.0:20005", privateKey, 2, onClient)
		<-finishCh
	})

	t.Run("it should transfer multiple files to destination", func(t *testing.T) {
		source1, err := os.Create("mf_source1.txt")
		assert.Nil(err)

		source2, err := os.Create("mf_source2.txt")
		assert.Nil(err)

		defer func() {
			source1.Close()
			source2.Close()
			os.Remove("mf_source1.txt")
			os.Remove("mf_source2.txt")
		}()

		source1.WriteString("source1 content")
		source2.WriteString("source2 content")

		currentDir, err := os.Getwd()
		assert.Nil(err)

		privateKey, err := testutils.GenerateRSAPrivateKey()
		assert.Nil(err)

		finishCh := make(chan struct{}, 1)
		destDir := currentDir + "/dest"

		if err = os.MkdirAll(destDir, 0755); err != nil {
			t.Error(err)
		}

		defer func() {
			if err = os.RemoveAll(destDir); err != nil {
				t.Error(err)
			}
		}()

		onClient := func() {
			cmd.SetArgs([]string{
				"sync", "sftp",
				"--source=" + currentDir, "--destination=" + destDir,
				"--ssh-host=127.0.0.1:20005",
				"--ssh-user=root",
				"--ssh-key=" + privateKey,
				"-p=mf_source*",
				"-v",
			})

			buf := make([]byte, 1024)
			readWriter := bytes.NewBuffer(buf)

			cmd.SetOut(readWriter)
			cmd.SetErr(readWriter)
			cmd.Execute()

			assert.Contains(readWriter.String(), "")
			finishCh <- struct{}{}
		}

		testutils.StartSftpServer("0.0.0.0:20005", privateKey, 3, onClient)
		<-finishCh
	})
}

package synccmd_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/liweiyi88/onedump/cmd"
	"github.com/liweiyi88/onedump/filesync"
	"github.com/liweiyi88/onedump/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncSftpCmd(t *testing.T) {
	cmd := cmd.RootCmd
	assert := assert.New(t)

	t.Run("it should return error if we try to sync multiple files but specify a single file as destination", func(t *testing.T) {
		currentDir, err := os.Getwd()
		assert.Nil(err)

		sourcePath := filepath.ToSlash(currentDir)
		destPath := filepath.ToSlash(filepath.Join(currentDir, "dest.txt"))

		finishCh := make(chan struct{}, 1)
		onServerReady := func(privateKey string) {
			cmd.SetArgs([]string{
				"sync", "sftp",
				"--source=" + sourcePath, "--destination=" + destPath,
				"--ssh-host=127.0.0.1:20005",
				"--ssh-user=root",
				"--ssh-key=" + privateKey,
			})

			buf := make([]byte, 1024)
			readWriter := bytes.NewBuffer(buf)

			cmd.SetOut(readWriter)
			cmd.SetErr(readWriter)
			assert.Error(cmd.Execute())

			assert.Contains(readWriter.String(), "Error: detination should not be a file when transfer multiple files from the source")
			finishCh <- struct{}{}
		}

		require.NoError(t, testutils.StartSftpServer("0.0.0.0:20005", 1, onServerReady))
		<-finishCh
	})

	t.Run("it should transfer a single file from source to destination", func(t *testing.T) {
		source, err := os.Create("single_source.txt")
		assert.Nil(err)

		defer func() {
			assert.NoError(source.Close())
			assert.NoError(os.Remove("single_source.txt"))
		}()

		_, err = source.WriteString("source content")
		assert.NoError(err)

		currentDir, err := os.Getwd()
		assert.Nil(err)

		finishCh := make(chan struct{}, 1)

		onServerReady := func(privateKey string) {
			destPath := filepath.ToSlash(filepath.Join(currentDir, "dest.txt"))
			defer func() { assert.NoError(os.Remove(destPath)) }()

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
			assert.NoError(cmd.Execute())

			assert.Equal(strings.ReplaceAll(readWriter.String(), "\x00", ""), "")
			finishCh <- struct{}{}
		}

		require.NoError(t, testutils.StartSftpServer("0.0.0.0:20005", 2, onServerReady))
		<-finishCh
	})

	t.Run("it should transfer multiple files to destination", func(t *testing.T) {
		source1, err := os.Create("mf_source1.txt")
		assert.Nil(err)

		source2, err := os.Create("mf_source2.txt")
		assert.Nil(err)

		defer func() {
			assert.NoError(source1.Close())
			assert.NoError(source2.Close())
			assert.NoError(os.Remove("mf_source1.txt"))
			assert.NoError(os.Remove("mf_source2.txt"))
		}()

		_, err = source1.WriteString("source1 content")
		assert.NoError(err)
		_, err = source2.WriteString("source2 content")
		assert.NoError(err)

		currentDir, err := os.Getwd()
		assert.Nil(err)

		finishCh := make(chan struct{}, 1)
		destDir := filepath.ToSlash(filepath.Join(currentDir, "/dest"))

		if err = os.MkdirAll(destDir, 0755); err != nil {
			t.Error(err)
		}

		defer func() {
			if err = os.RemoveAll(destDir); err != nil {
				t.Error(err)
			}
		}()

		onServerReady := func(privateKey string) {

			sourcePath := filepath.ToSlash(currentDir)

			cmd.SetArgs([]string{
				"sync", "sftp",
				"--source=" + sourcePath, "--destination=" + destDir,
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
			assert.NoError(cmd.Execute())

			assert.Equal(strings.ReplaceAll(readWriter.String(), "\x00", ""), "")
			finishCh <- struct{}{}
		}

		require.NoError(t, testutils.StartSftpServer("0.0.0.0:20005", 3, onServerReady))
		<-finishCh
	})

	t.Run("it should save checksum state file when --checksum=true option is passed", func(t *testing.T) {
		source, err := os.Create("single_source.txt")
		assert.Nil(err)

		defer func() {
			assert.NoError(source.Close())
			assert.NoError(os.Remove("single_source.txt"))
		}()

		_, err = source.WriteString("source content")
		assert.NoError(err)

		currentDir, err := os.Getwd()
		assert.Nil(err)

		finishCh := make(chan struct{}, 1)

		onServerReady := func(privateKey string) {
			destPath := currentDir + "/dest.txt"
			defer func() { assert.NoError(os.Remove(destPath)) }()

			cmd.SetArgs([]string{
				"sync", "sftp",
				"--source=" + source.Name(), "--destination=" + destPath,
				"--ssh-host=127.0.0.1:20005",
				"--ssh-user=root",
				"--ssh-key=" + privateKey,
				"--checksum=true",
				"-v",
			})

			buf := make([]byte, 1024)
			readWriter := bytes.NewBuffer(buf)

			cmd.SetOut(readWriter)
			cmd.SetErr(readWriter)
			assert.NoError(cmd.Execute())

			assert.Equal(strings.ReplaceAll(readWriter.String(), "\x00", ""), "")
			finishCh <- struct{}{}

			wd, err := os.Getwd()
			assert.Nil(err)

			checksumState, err := os.Stat(filepath.Join(wd, filesync.ChecksumStateFile))
			assert.Nil(err)
			assert.Equal(checksumState.Size(), int64(64))

			defer func() {
				if err = os.Remove(filesync.ChecksumStateFile); err != nil {
					t.Error(err)
				}
			}()
		}

		require.NoError(t, testutils.StartSftpServer("0.0.0.0:20005", 2, onServerReady))
		<-finishCh
	})

	t.Run("it should not transfer file that has already been transfered when --checksum=true option is passed", func(t *testing.T) {
		source, err := os.Create("single_source.txt")
		assert.Nil(err)

		defer func() {
			assert.NoError(source.Close())
			assert.NoError(os.Remove("single_source.txt"))
		}()

		_, err = source.WriteString("source content")
		assert.NoError(err)

		checksum := filesync.NewChecksum(source.Name(), "")
		err = checksum.SaveState()
		assert.Nil(err)

		defer func() {
			if err := checksum.DeleteState(); err != nil {
				t.Error(err)
			}
		}()

		currentDir, err := os.Getwd()
		assert.Nil(err)

		finishCh := make(chan struct{}, 1)

		onServerReady := func(privateKey string) {
			destPath := currentDir + "/dest.txt"

			cmd.SetArgs([]string{
				"sync", "sftp",
				"--source=" + source.Name(), "--destination=" + destPath,
				"--ssh-host=127.0.0.1:20005",
				"--ssh-user=root",
				"--ssh-key=" + privateKey,
				"--checksum=true",
				"-v",
			})

			buf := make([]byte, 1024)
			readWriter := bytes.NewBuffer(buf)

			cmd.SetOut(readWriter)
			cmd.SetErr(readWriter)
			assert.NoError(cmd.Execute())

			assert.Equal(strings.ReplaceAll(readWriter.String(), "\x00", ""), "")
			assert.NoFileExists(destPath)
			finishCh <- struct{}{}
		}

		require.NoError(t, testutils.StartSftpServer("0.0.0.0:20005", 1, onServerReady))
		<-finishCh
	})
}

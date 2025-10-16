package cmd

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRootCmdWithCron(t *testing.T) {
	assert := assert.New(t)
	cmd := RootCmd

	workDir, _ := os.Getwd()
	filename := workDir + "/test.sql"
	file, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(filename)
	file.Close()

	config := `
maxjobs: 20	
jobs:
- name: local-dump
  dbdriver: mysql
  dbdsn: root@tcp(127.0.0.1)/unknow
  gzip: true
  storage:
    local:
    - path: /Users/julianli/Desktop/test-local.sql
`

	newFd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.ModePerm)
	assert.Nil(err)

	err = os.WriteFile(filename, []byte(config), 0644)
	assert.Nil(err)

	newFd.Close()
	o := bytes.NewBufferString("")
	cmd.SetOut(o)
	cmd.SetArgs([]string{"-f", filename, "-c", "1s"})
	err = cmd.Execute()
	assert.Error(err)

	cmd.SetArgs([]string{"-f", filename, "-c", "10s"})
	err = cmd.Execute()

	assert.Error(err)
}

func TestRootCmdWithInvalidMaxJob(t *testing.T) {
	assert := assert.New(t)
	cmd := RootCmd

	workDir, _ := os.Getwd()
	filename := workDir + "/test.sql"
	file, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(filename)
	file.Close()

	config := `
maxjobs: 0	
jobs:
- name: local-dump
  dbdriver: mysql
  dbdsn: root@tcp(127.0.0.1)/unknow
  gzip: true
  storage:
    local:
    - path: /Users/julianli/Desktop/test-local.sql
`

	newFd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.ModePerm)
	assert.Nil(err)

	err = os.WriteFile(filename, []byte(config), 0644)
	assert.Nil(err)

	newFd.Close()
	o := bytes.NewBufferString("")
	cmd.SetOut(o)
	cmd.SetArgs([]string{"-f", filename, "-c", "1s"})
	err = cmd.Execute()
	assert.Error(err)
	assert.Equal("invalid job configuration, error: max jobs should be greater than 0, got 0", err.Error())
}

func TestRootCmd(t *testing.T) {
	assert := assert.New(t)

	t.Run("it should return error when job file can not be opened", func(t *testing.T) {
		cmd := RootCmd
		cmd.SetArgs([]string{"-f", "/Users/jobs.yaml"})
		b := bytes.NewBufferString("")
		cmd.SetErr(b)
		err := cmd.Execute()
		assert.NotNil(err)

		out, err := io.ReadAll(b)
		if err != nil {
			t.Fatal(err)
		}

		expect := "Error: failed to read job file from /Users/jobs.yaml, error: open /Users/jobs.yaml:"
		actual := string(out)
		assert.True(strings.HasPrefix(strings.TrimSpace(actual), expect))
	})

	t.Run("it should return error when no job is defined in the job config file", func(t *testing.T) {
		t.Setenv("AWS_REGION", "ap-southeast-2")
		t.Setenv("AWS_ACCESS_KEY_ID", "accessk-key-id")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-access-key")

		workDir, _ := os.Getwd()
		filename := workDir + "/test.sql"
		file, err := os.Create(filename)
		assert.Nil(err)

		defer os.Remove(filename)
		file.Close()

		cmd := RootCmd
		cmd.SetArgs([]string{"-f", filename})
		b := bytes.NewBufferString("")
		cmd.SetErr(b)
		cmd.Execute()

		out, err := io.ReadAll(b)
		assert.Nil(err)

		expect := "Error: no job is defined in the file " + filename
		actual := string(out)

		assert.Equal(expect, strings.TrimSpace(actual))

		newFd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.ModePerm)
		assert.Nil(err)

		config := `jobs:
- name: local-dump
  dbdriver: mysql
  dbdsn: root@tcp(127.0.0.1)/unknow
  gzip: true
  storage:
    local:
    - path: /Users/julianli/Desktop/test-local.sql`

		err = os.WriteFile(filename, []byte(config), 0644)
		assert.Nil(err)

		newFd.Close()
		o := bytes.NewBufferString("")
		cmd.SetOut(o)
		err = cmd.Execute()
		assert.NotNil(err)
	})
}

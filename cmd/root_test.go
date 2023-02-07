package cmd

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestRootCmd(t *testing.T) {
	cmd := rootCmd
	cmd.SetArgs([]string{"-f", "/Users/jobs.yaml"})
	b := bytes.NewBufferString("")
	cmd.SetErr(b)
	cmd.Execute()

	out, err := io.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}

	expect := "Error: failed to read job file from /Users/jobs.yaml, error: open /Users/jobs.yaml:"
	actual := string(out)

	if !strings.HasPrefix(strings.TrimSpace(actual), expect) {
		t.Errorf("expected: %v, but actual get: %s", expect, actual)
	}

	workDir, _ := os.Getwd()
	filename := workDir + "/test.sql"
	file, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(filename)
	file.Close()

	cmd.SetArgs([]string{"-f", filename})
	cmd.Execute()

	out, err = io.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}

	expect = "Error: no job is defined in the file " + filename
	actual = string(out)

	if strings.TrimSpace(actual) != expect {
		t.Errorf("expected: %v, but actual get: %s", expect, actual)
	}

	newFd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	config := `jobs:
- name: local-dump
  dbdriver: mysql
  dbdsn: root@tcp(127.0.0.1)/unknow
  gzip: true
  storage:
    local:
    - path: /Users/julianli/Desktop/test-local.sql
`

	err = os.WriteFile(filename, []byte(config), 0644)

	if err != nil {
		t.Fatal(err)
	}

	newFd.Close()
	o := bytes.NewBufferString("")
	cmd.SetOutput(o)
	err = cmd.Execute()

	if err == nil {
		t.Error("expect errr but got nil")
	}
}

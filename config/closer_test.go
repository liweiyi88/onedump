package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockCloser struct{}

func (m mockCloser) Close() error {
	fmt.Print("close")
	return nil
}

func TestNewMultiCloser(t *testing.T) {

	closer1 := mockCloser{}
	closer2 := mockCloser{}

	closers := make([]io.Closer, 0)
	closers = append(closers, closer1, closer2)

	multiCloser := NewMultiCloser(closers)

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	multiCloser.Close()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	expected := buf.String()
	actual := "closeclose"

	assert.Equal(t, expected, actual)
}

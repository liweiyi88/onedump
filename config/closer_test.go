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

	assert.NoError(t, multiCloser.Close())

	assert.NoError(t, w.Close())
	os.Stdout = old

	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	assert.NoError(t, err)

	expected := buf.String()
	actual := "closeclose"

	assert.Equal(t, expected, actual)
}

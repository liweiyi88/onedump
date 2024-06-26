package local

import (
	"os"
	"strings"
	"testing"

	"github.com/liweiyi88/onedump/storage"
)

func TestSave(t *testing.T) {
	filename := os.TempDir() + "/test.sql.gz"
	local := &Local{Path: filename}

	expected := "hello"
	reader := strings.NewReader(expected)

	err := local.Save(reader, storage.PathGenerator(true, false))
	if err != nil {
		t.Errorf("failed to save file: %v", err)
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Errorf("can not read file %s", err)
	}

	if string(data) != expected {
		t.Errorf("expected string: %s but actual got: %s", expected, data)
	}

	defer os.Remove(filename)
}

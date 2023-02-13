package fileutil

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEnsureFileName(t *testing.T) {
	p := EnsureFileName("/Users/jack/Desktop/hello.sql", true, false)

	if p != "/Users/jack/Desktop/hello.sql.gz" {
		t.Errorf("unexpected filename: %s", p)
	}
}

func TestEnsureFileSuffix(t *testing.T) {
	f := EnsureFileSuffix("test.sql", true)

	if f != "test.sql.gz" {
		t.Errorf("expected .gz extension but got: %s", f)
	}

	f = EnsureFileSuffix("test.sql.gz", true)

	if f != "test.sql.gz" {
		t.Errorf("expected .gz extension but got: %s", f)
	}

	f = EnsureFileSuffix("test.sql", false)

	if f != "test.sql" {
		t.Errorf("expected .sql extension but got: %s", f)
	}
}

func TestEnsureUniqueness(t *testing.T) {
	path := "/Users/jack/Desktop/hello.sql"

	p := ensureUniqueness(path, false)
	if path != p {
		t.Errorf("expected same paths but got %s", p)
	}

	p = ensureUniqueness(path, true)

	_, filename := filepath.Split(p)

	now := time.Now().UTC().Format("2006010215")

	if !strings.HasPrefix(filename, now) {
		t.Errorf("got incorrect filename prefix: %s", filename)
	}

	if !strings.HasSuffix(filename, "-hello.sql") {
		t.Errorf("got incorrect filename suffix: %s", filename)
	}
}

func TestGenerateRandomName(t *testing.T) {
	n := GenerateRandomName(10)

	if len(n) != 10 {
		t.Errorf("expect length 10: but got: %d", len(n))
	}
}

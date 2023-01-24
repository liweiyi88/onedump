package filenaming

import (
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

func TestEnsureUniqueness(t *testing.T) {
	path := "/Users/jack/Desktop/hello.sql"

	p := ensureUniqueness("/Users/jack/Desktop/hello.sql", false)
	if path != p {
		t.Errorf("expected same paths but got %s", p)
	}

	p = ensureUniqueness("/Users/jack/Desktop/hello.sql", true)

	if !strings.HasPrefix(p, "/Users/jack/Desktop/") {
		t.Errorf("got incorrect path: %s", p)
	}

	s := strings.Split(p, "/")
	filename := s[len(s)-1]

	now := time.Now().UTC().Format("2006010215")

	if !strings.HasPrefix(filename, now) {
		t.Errorf("got incorrect filename prefix: %s", filename)
	}

	if !strings.HasSuffix(filename, "-hello.sql") {
		t.Errorf("got incorrect filename suffix: %s", filename)
	}
}

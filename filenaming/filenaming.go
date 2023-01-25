package filenaming

import (
	"strings"
	"time"
)

// Ensure a file has proper file extension.
func EnsureFileSuffix(filename string, shouldGzip bool) string {
	if !shouldGzip {
		return filename
	}

	if strings.HasSuffix(filename, ".gz") {
		return filename
	}

	return filename + ".gz"
}

// Ensure a file has unique name when necessary.
func ensureUniqueness(path string, unique bool) string {
	if !unique {
		return path
	}

	s := strings.Split(path, "/")

	filename := s[len(s)-1]
	now := time.Now().UTC().Format("20060102150405")
	uniqueFile := now + "-" + filename

	s[len(s)-1] = uniqueFile

	return strings.Join(s, "/")
}

func EnsureFileName(path string, shouldGzip, unique bool) string {
	p := EnsureFileSuffix(path, shouldGzip)
	return ensureUniqueness(p, unique)
}

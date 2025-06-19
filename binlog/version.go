package binlog

import (
	"log/slog"
	"strconv"
	"strings"
	"unicode"
)

type mysqlVersion struct {
	major, minor, patch int
}

// It parses a MySQL version string (e.g., "8.0.34") into major, minor, and patch numbers.
func splitServerVersion(version string) *mysqlVersion {
	if version == "" {
		return &mysqlVersion{major: 0, minor: 0, patch: 0}
	}

	parts := strings.Split(version, ".")

	var major, minor, patch int

	for i, v := range parts {
		if i == 0 {
			n, err := strconv.Atoi(v)
			if err != nil {
				slog.Info("failed to parse major version number", slog.String("version", version), slog.Any("error", err))
				return &mysqlVersion{major: 0, minor: 0, patch: 0}
			}

			major = n
		} else if i == 1 {
			n, err := strconv.Atoi(v)

			// if minor version string is not a number, then we try to extract number and ignore the patch part.
			if err != nil {
				minor = extractNumber(v)
				return &mysqlVersion{
					major: major,
					minor: minor,
					patch: 0,
				}
			}

			minor = n
		} else if i == 2 {
			patch = extractNumber(v)
		}
	}

	return &mysqlVersion{major, minor, patch}
}

func extractNumber(v string) int {
	var numStr string

	for _, ch := range v {
		if unicode.IsNumber(ch) {
			numStr += string(ch)
		} else if len(numStr) > 0 {
			break
		}
	}

	if numStr == "" {
		return 0
	}

	n, err := strconv.Atoi(numStr)
	if err != nil {
		return 0
	}

	return n
}

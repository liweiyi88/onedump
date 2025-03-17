package binlog

import (
	"strconv"
	"strings"
	"unicode"
)

// see https://github.com/mysql/mysql-server/blob/6b6d3ed3d5c6591b446276184642d7d0504ecc86/libs/mysql/binlog/event/binlog_event.cpp#L34
func calculateVersionProduct(version string) int {
	major, minor, patch := splitServerVersion(version)

	return (major*256+minor)*256 + patch
}

// It parses a MySQL version string (e.g., "8.0.34") into major, minor, and patch numbers.
// It is a rewrite of https://github.com/mysql/mysql-server/blob/6b6d3ed3d5c6591b446276184642d7d0504ecc86/libs/mysql/binlog/event/binlog_event.h#L184
func splitServerVersion(version string) (int, int, int) {
	parts := strings.Split(version, ".")

	var major, minor, patch int

	for i, v := range parts {
		if i == 0 {
			n, err := strconv.Atoi(v)
			if err != nil {
				return 0, 0, 0
			}

			major = n
		} else if i == 1 {
			n, err := strconv.Atoi(v)

			// if minor version string is not a number, then we try to extract number and ignore the patch part.
			if err != nil {
				minor = extractNumber(v)
				return major, minor, 0
			}

			minor = n
		} else if i == 2 {
			patch = extractNumber(v)
		}
	}

	return major, minor, patch
}

func extractNumber(v string) int {
	var numStr string

	for _, ch := range v {
		if unicode.IsNumber(ch) {
			numStr += string(ch) // Collect digits
		} else if len(numStr) > 0 {
			break // Stop at the first non-digit after starting
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

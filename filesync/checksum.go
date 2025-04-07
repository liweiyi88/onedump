package filesync

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const ChecksumStateFile = "checksum.onedump"

type Checksum struct {
	filePath string
	mu       sync.Mutex
}

func NewChecksum(filePath string) *Checksum {
	return &Checksum{filePath: filePath}
}

func (c *Checksum) computeChecksum() (string, error) {
	hasher := sha256.New()

	file, err := os.Open(c.filePath)

	if err != nil {
		return "", fmt.Errorf("fail to open file %s to compute checksum, error: %v", c.filePath, err)
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			slog.Error("fail to close file", slog.Any("filename", file.Name()), slog.Any("error", closeErr))
		}
	}()

	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("fail to copy content to hasher, error: %v", err)
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (c *Checksum) getStateFilePath() string {
	return filepath.Join(filepath.Dir(c.filePath), ChecksumStateFile)
}

func (c *Checksum) IsFileTransferred() (bool, error) {
	checksum, err := c.computeChecksum()
	if err != nil {
		return false, err
	}

	_, err = os.Stat(c.getStateFilePath())

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, fmt.Errorf("fail to inspect checksum file stat, error: %v", err)
	}

	// We store the checksum state file in the same directory as the file
	stateFile, err := os.OpenFile(c.getStateFilePath(), os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return false, fmt.Errorf("fail to open checksum file: %v", err)
	}

	defer func() {
		if closeErr := stateFile.Close(); closeErr != nil {
			slog.Error("fail to close the checksum state file while checking if file has been transferred", slog.Any("error", closeErr))
		}
	}()

	// SHA-256 checksum is always 64 characters in hex format, so using bufio scanner is simple and safe for this case.
	scanner := bufio.NewScanner(stateFile)

	for scanner.Scan() {
		if checksum == strings.TrimSpace(scanner.Text()) {
			return true, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("fail to scan file, error: %v", err)
	}

	return false, nil
}

func (c *Checksum) DeleteState() error {
	err := os.Remove(c.getStateFilePath())

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (c *Checksum) SaveState() error {
	file, err := os.OpenFile(c.getStateFilePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return fmt.Errorf("fail to open state file while saving, error: %v", err)
	}

	defer func() {
		if err = file.Close(); err != nil {
			slog.Error("fail to close state file while saving", slog.Any("error", err))
		}
	}()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("fail to get file info while saving, error: %v", err)
	}

	checksum, err := c.computeChecksum()
	if err != nil {
		return fmt.Errorf("fail to get checksum while saving, error: %v", err)
	}

	var content = checksum

	if fileInfo.Size() > 0 {
		content = "\n" + checksum
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err = file.WriteString(content); err != nil {
		return fmt.Errorf("fail to write checksum while saving, error: %v", err)
	}

	return nil
}

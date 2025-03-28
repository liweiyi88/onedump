package fileutil

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

const checksumStateFile = "checksum.onedump"

type Checksum struct {
	filePath string
	checksum string
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
		if err = file.Close(); err != nil {
			slog.Error("fail to close file", slog.Any("filename", file.Name()), slog.Any("error", err))
		}
	}()

	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("fail to copy content to hasher, error: %v", err)
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (c *Checksum) getStateFilePath() string {
	return filepath.Join(filepath.Dir(c.filePath), checksumStateFile)
}

// We store the checksum state file in the same directory as the file
func (c *Checksum) getChecksum() (string, error) {
	if len(c.checksum) == 64 {
		return c.checksum, nil
	}

	checksum, err := c.computeChecksum()

	if err != nil {
		return "", fmt.Errorf("fail to compute checksum for file %s, error: %v", c.filePath, err)
	}

	c.checksum = checksum

	return checksum, nil
}

func (c *Checksum) IsFileTransferred() (bool, error) {
	checksum, err := c.getChecksum()
	if err != nil {
		return false, err
	}

	// We store the checksum state file in the same directory as the file
	stateFile, err := os.OpenFile(c.getStateFilePath(), os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return false, fmt.Errorf("failed to open checksum file: %v", err)
	}

	defer func() {
		if err := stateFile.Close(); err != nil {
			slog.Error("fail to close the checksum state file while checking if file has been transfered", slog.Any("error", err))
		}
	}()

	// Checksum is always 64 bits, so use bufio scanner is simple and safe for this case.
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

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

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

	checksum, err := c.getChecksum()
	if err != nil {
		return fmt.Errorf("fail to get checksum while saving, error: %v", err)
	}

	var content = checksum

	if fileInfo.Size() > 0 {
		content = "\n" + checksum
	}

	if _, err = file.WriteString(content); err != nil {
		return fmt.Errorf("fail to write checksum while saving, error: %v", err)
	}

	return nil
}

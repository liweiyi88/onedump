package filesync

import (
	"fmt"
	"log/slog"
)

func HasSynced(filename string) (bool, error) {
	synced, err := NewChecksum(filename).IsFileTransferred()

	if err != nil {
		return false, fmt.Errorf("fail to check if %s has been transferred, error: %v", filename, err)
	}

	return synced, nil
}

func SyncFile(filename string, checksum bool, syncFunc func() error) error {
	fileChecksum := NewChecksum(filename)

	if !checksum {
		if err := fileChecksum.DeleteState(); err != nil {
			return fmt.Errorf("fail to delete the checksum state file, error: %v", err)
		}
	} else {
		transfered, err := fileChecksum.IsFileTransferred()
		if err != nil {
			return err
		}

		if transfered {
			slog.Debug("the file has already been transferred", slog.Any("filename", filename))
			return nil
		}
	}

	if err := syncFunc(); err != nil {
		return err
	}

	if checksum {
		if err := fileChecksum.SaveState(); err != nil {
			return fmt.Errorf("fail to save the checksum state file for %s, error: %v", filename, err)
		}
	}

	return nil
}

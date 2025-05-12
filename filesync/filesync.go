package filesync

import (
	"fmt"
	"log/slog"
)

type FileSync struct {
	SaveChecksum bool
	checksumFile string
}

func NewFileSync(saveChecksum bool, checksumFile string) *FileSync {
	return &FileSync{
		SaveChecksum: saveChecksum,
		checksumFile: checksumFile,
	}
}

func (fs *FileSync) HasSynced(filename string) (bool, error) {
	synced, err := NewChecksum(filename, fs.checksumFile).IsFileTransferred()

	if err != nil {
		return false, fmt.Errorf("fail to check if %s has been transferred, error: %v", filename, err)
	}

	return synced, nil
}

func (fs *FileSync) SyncFile(filename string, syncFunc func() error) error {
	fileChecksum := NewChecksum(filename, fs.checksumFile)

	if !fs.SaveChecksum {
		if err := fileChecksum.DeleteState(); err != nil {
			return fmt.Errorf("fail to delete the checksum state file, error: %v", err)
		}
	} else {
		transfered, err := fileChecksum.IsFileTransferred()
		if err != nil {
			return err
		}

		if transfered {
			slog.Debug("[filesync] the file has already been transferred", slog.Any("filename", filename))
			return nil
		}
	}

	if err := syncFunc(); err != nil {
		return err
	}

	if fs.SaveChecksum {
		if err := fileChecksum.SaveState(); err != nil {
			return fmt.Errorf("fail to save the checksum state file for %s, error: %v", filename, err)
		}
	}

	return nil
}

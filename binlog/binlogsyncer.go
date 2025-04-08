package binlog

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/liweiyi88/onedump/filesync"
	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/storage"
)

const MaxConcurrentSync = 10

type BinlogSyncer struct {
	destinationPath string // storage folder
	checksum        bool   // if save checksum and avoid re-transfer
	*BinlogInfo
}

func (b *BinlogSyncer) syncFile(filename string, storage storage.Storage) error {
	syncFunc := func() error {
		f, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("fail to open file: %s, error %v", filename, err)
		}

		defer func() {
			if err := f.Close(); err != nil {
				slog.Error("fail to close file.", slog.Any("file", f.Name()), slog.Any("error", err))
			}
		}()

		s, err := f.Stat()
		if err != nil {
			return fmt.Errorf("fail to get file stat: %s, error %v", filename, err)
		}

		pathGenerator := func(filename string) string {
			return b.destinationPath + "/" + s.Name()
		}

		if err = storage.Save(f, pathGenerator); err != nil {
			return fmt.Errorf("fail to save file to destination, error: %v", err)
		}

		return nil
	}

	return filesync.SyncFile(filename, b.checksum, syncFunc)
}

func (b *BinlogSyncer) Sync(storage storage.Storage) error {
	files, err := fileutil.ListFiles(b.binlogDir, b.binlogPrefix+"*")
	if err != nil {
		return fmt.Errorf("fail to list all binlog files, error: %v", err)
	}

	limiter := make(chan struct{}, MaxConcurrentSync)
	errCh := make(chan error, len(files))
	var wg sync.WaitGroup

	for _, file := range files {
		wg.Add(1)
		limiter <- struct{}{}

		go func() {
			defer func() {
				<-limiter
				wg.Done()
			}()

			if err := b.syncFile(file, storage); err != nil {
				errCh <- err
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var allErrors []error
	for err := range errCh {
		allErrors = append(allErrors, err)
	}

	return errors.Join(allErrors...)
}

func NewBinlogSyncer(destinationPath string, checksum bool, binlogInfo *BinlogInfo) *BinlogSyncer {
	return &BinlogSyncer{
		destinationPath,
		checksum,
		binlogInfo,
	}
}

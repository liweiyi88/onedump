package binlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/liweiyi88/onedump/filesync"
	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/storage"
)

const (
	MaxConcurrentSync = 10
	SyncResultFile    = "onedump-binlog-sync.log" // The default binlog sync result filename
)

type syncResult struct {
	FinishAt time.Time `json:"finished_at"`
	Files    []string  `json:"files"`
	Ok       bool      `json:"ok"`
	Error    string    `json:"error"`
}

func newSyncResult(files []string, err error) *syncResult {
	var syncError string
	var ok bool
	if err != nil {
		syncError = err.Error()
	} else {
		ok = true
	}

	return &syncResult{
		Files:    files,
		Error:    syncError,
		FinishAt: time.Now().UTC(),
		Ok:       ok,
	}
}

func (s *syncResult) save(dir string, logFile string) error {
	encoded, err := json.Marshal(s)

	if err != nil {
		return fmt.Errorf("fail to encode sync result to json, error: %v", err)
	}

	resultFile := filepath.Join(dir, SyncResultFile)

	if strings.TrimSpace(logFile) != "" {
		resultFile = logFile
	}

	// Ensure the directory for the result file exists
	if err := os.MkdirAll(filepath.Dir(resultFile), 0o755); err != nil {
		return fmt.Errorf("fail to create result log directory, error: %v", err)
	}

	syncFile, err := os.OpenFile(resultFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("fail to open sync result file, error: %v", err)
	}

	defer func() {
		if err := syncFile.Close(); err != nil {
			slog.Error("fail to close sync file", slog.Any("filename", syncFile.Name()), slog.Any("error", err))
		}
	}()

	data := append(encoded, []byte("\n")...)
	_, err = syncFile.Write(data)

	return err
}

type BinlogSyncer struct {
	destinationPath string // storage folder
	saveLog         bool   // is save sync result in a log file
	logFile         string // if not empty string, save result log in the specific file.
	fs              *filesync.FileSync
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

		// binlog file can be updated during upload (MySQL flush logs).
		// Enforce the size based on the current read for consistency.
		limitedReader := io.LimitReader(f, s.Size())

		pathGenerator := func(filename string) string {
			return b.destinationPath + "/" + s.Name()
		}

		if err = storage.Save(limitedReader, pathGenerator); err != nil {
			return fmt.Errorf("fail to save file to destination, error: %v", err)
		}

		return nil
	}

	return b.fs.SyncFile(filename, syncFunc)
}

func (b *BinlogSyncer) Sync(storage storage.Storage) error {
	files, err := fileutil.ListFiles(b.binlogDir, b.binlogPrefix+"*")
	if err != nil {
		return fmt.Errorf("fail to list all binlog files, error: %v", err)
	}

	limiter := make(chan struct{}, MaxConcurrentSync)
	errCh := make(chan error, len(files))
	var wg sync.WaitGroup

	var syncFiles []string
	// Filter out files that have been synced before.
	// So the save log will persist proper file names.
	if b.fs.SaveChecksum {
		for _, file := range files {
			synced, err := b.fs.HasSynced(file)

			if err != nil {
				return fmt.Errorf("fail to check if %s has been transferred, error: %v", file, err)
			}

			if !synced {
				syncFiles = append(syncFiles, file)
			}
		}
	} else {
		syncFiles = files
	}

	for _, file := range syncFiles {
		wg.Add(1)
		limiter <- struct{}{}

		go func(file string) {
			defer func() {
				<-limiter
				wg.Done()
			}()

			if err := b.syncFile(file, storage); err != nil {
				errCh <- fmt.Errorf("fail to sync file: %s, error: %v", file, err)
			}
		}(file)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var allErrors []error
	for err := range errCh {
		allErrors = append(allErrors, err)
	}

	syncError := errors.Join(allErrors...)

	if !b.saveLog {
		return syncError
	}

	saveErr := newSyncResult(syncFiles, syncError).save(b.binlogDir, b.logFile)
	if saveErr != nil {
		return errors.Join(syncError, saveErr)
	}

	return syncError
}

func NewBinlogSyncer(
	destinationPath string,
	saveLog bool,
	logFile string,
	fileSync *filesync.FileSync,
	binlogInfo *BinlogInfo,
) *BinlogSyncer {
	return &BinlogSyncer{
		destinationPath: destinationPath,
		saveLog:         saveLog,
		logFile:         logFile,
		fs:              fileSync,
		BinlogInfo:      binlogInfo,
	}
}

package cmd

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/storage/sftp"
	"github.com/spf13/cobra"
)

func syncFiles(sources []string, destination string, checksum bool, isDestinationDir bool, config *sftp.SftpConifg) error {
	errCh := make(chan error, len(sources))

	// Process maximum 10 files at a time
	semaphore := make(chan struct{}, 10)

	var wg sync.WaitGroup
	for _, file := range sources {
		wg.Add(1)
		semaphore <- struct{}{}
		go func() {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			err := syncFile(file, destination, checksum, isDestinationDir, config)
			if err != nil {
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

func syncFile(source, destination string, checksum bool, isDestinationDir bool, config *sftp.SftpConifg) error {
	fileChecksum := fileutil.NewChecksum(source)

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
			slog.Debug("the file has already been transfered", slog.Any("filename", source))
			return nil
		}
	}

	sourceFile, err := os.Open(source)

	if err != nil {
		return fmt.Errorf("fail to open the source file: %s, error: %v", source, err)
	}

	defer func() {
		if err := sourceFile.Close(); err != nil {
			slog.Error("fail to close the source file", slog.Any("error", err))
		}
	}()

	path := destination
	if isDestinationDir {
		sourceFileInfo, err := sourceFile.Stat()
		if err != nil {
			return fmt.Errorf("fail to get source file stat, error: %v", err)
		}

		path = filepath.Join(destination, sourceFileInfo.Name())
	}

	sftp := sftp.NewSftp(config)
	err = sftp.Save(sourceFile, func(filename string) string { return path })

	if err != nil {
		return fmt.Errorf("fail to sync file %s to destination %s, error: %v", source, destination, err)
	}

	if checksum {
		err := fileChecksum.SaveState()
		if err != nil {
			return fmt.Errorf("fail to save the checksum state file for %s, error: %v", source, err)
		}
	}

	return nil
}

var syncSftpCmd = &cobra.Command{
	Use:   "sync sftp",
	Short: "Resumable and concurrent SFTP files transfer.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if verbose {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		config := &sftp.SftpConifg{
			Host:        sftpHost,
			User:        sftpUser,
			Key:         sftpKey,
			MaxAttempts: sftpMaxAttempts,
		}

		if config.Host == "" || config.User == "" || config.Key == "" {
			return errors.New("ssh host, user, and key are required for SFTP connection")
		}

		isDestinationDir, err := sftp.NewSftp(config).IsPathDir(destination)
		if err != nil {
			return fmt.Errorf("fail to check if destination is directory, error: %v", err)
		}

		sourceInfo, err := os.Stat(source)
		if err != nil {
			return fmt.Errorf("fail to get source info %v", err)
		}

		if sourceInfo.IsDir() && !isDestinationDir {
			return errors.New("detination should not be a file when transfer multiple files from the source")
		}

		if sourceInfo.IsDir() {
			files, err := fileutil.ListFiles(source, pattern)
			if err != nil {
				return fmt.Errorf("fail to list all files for source dir, error: %v", err)
			}

			if len(files) == 0 {
				return errors.New("no file found for syncing")
			}

			return syncFiles(files, destination, checksum, isDestinationDir, config)
		}

		return syncFile(source, destination, checksum, isDestinationDir, config)
	},
}

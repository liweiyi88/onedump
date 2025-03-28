package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/storage/sftp"
	"github.com/spf13/cobra"
)

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
	Short: "Sync files to a remote server via SSH with resumable transfer and retries.",
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

		isDestinationDir, err := sftp.NewSftp(config).IsPathDir(destination)
		if err != nil {
			return fmt.Errorf("fail to check if destination is directory, error: %v", err)
		}

		return syncFile(source, destination, checksum, isDestinationDir, config)
	},
}

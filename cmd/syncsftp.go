package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/storage/sftp"
	"github.com/spf13/cobra"
)

var syncSftpCmd = &cobra.Command{
	Use:   "sync sftp",
	Short: "Sync files to a remote server via SSH with resumable transfer and retries.",
	RunE: func(cmd *cobra.Command, args []string) error {
		sourceFile, err := os.Open(source)

		if err != nil {
			return fmt.Errorf("fail to open source file: %s, error: %v", source, err)
		}

		defer func() {
			if err := sourceFile.Close(); err != nil {
				slog.Error("fail to close source file", slog.Any("error", err))
			}
		}()

		if verbose {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		fileChecksum := fileutil.NewChecksum(sourceFile)

		if checksum {
			transfered, err := fileChecksum.IsFileTransferred()
			if err != nil {
				return err
			}

			if transfered {
				slog.Debug("the file has already been transfered", slog.Any("filename", source))
				return nil
			}
		}

		err = sftp.
			NewSftp(sftpMaxAttempts, destination, sftpHost, sftpUser, sftpKey).
			Save(sourceFile, nil)

		if err != nil {
			return fmt.Errorf("fail to sync file %s to destination %s, error: %v", source, destination, err)
		}

		if checksum {
			err := fileChecksum.SaveState()
			if err != nil {
				return fmt.Errorf("fail to save checksum state file for %s, error: %v", source, err)
			}
		}

		return nil
	},
}

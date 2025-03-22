package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/liweiyi88/onedump/storage/sftp"
	"github.com/spf13/cobra"
)

var syncSftpCmd = &cobra.Command{
	Use:   "sync sftp",
	Short: "Sync files to a remote server via SSH with resumable transfer and retries.",
	RunE: func(cmd *cobra.Command, args []string) error {
		sourceFile, err := os.Open(source)
		if err != nil {
			return fmt.Errorf("fail to open source file %s: %w", source, err)
		}

		defer func() {
			if err := sourceFile.Close(); err != nil {
				slog.Error("fail to close the source file", slog.Any("error", err))
			}
		}()

		if verbose {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		sftp := sftp.NewSftp(sftpMaxAttempts, destination, sftpHost, sftpUser, sftpKey)

		pathGenerator := func(filename string) string {
			return filename
		}

		return sftp.Save(sourceFile, pathGenerator)
	},
}

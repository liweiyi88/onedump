package cmd

import (
	"fmt"
	"os"

	"github.com/liweiyi88/onedump/storage/sftp"
	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync a file to a remote server via SSH with resumable transfer and retries.",
	RunE: func(cmd *cobra.Command, args []string) error {
		file, err := os.Open(source)
		if err != nil {
			return fmt.Errorf("fail to open source file: %s", source)
		}

		sftp := sftp.NewSftp(syncMaxAttempts, destination, syncHost, syncUser, syncKey)

		pathGenerator := func(filename string) string {
			return filename
		}

		err = sftp.Save(file, pathGenerator)

		// @Todo: SFTP afterSave function to save result somewhere
		return err
	},
}

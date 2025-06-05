package synccmd

import (
	"github.com/spf13/cobra"
)

func init() {
	SyncCmd.AddCommand(SyncSftpCmd)
}

var SyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync files from source to destination.",
}

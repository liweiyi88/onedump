package downloadcmd

import "github.com/spf13/cobra"

func init() {
	DownloadCmd.AddCommand(DownloadS3Cmd)
}

var DownloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download files from storage to a local folder",
}

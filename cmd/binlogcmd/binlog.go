package binlogcmd

import (
	"database/sql"

	"github.com/spf13/cobra"
)

var (
	s3Bucket, s3Prefix string
	dryRun, verbose    bool
)

var OpenDB = func(dsn string) (*sql.DB, error) {
	return sql.Open("mysql", dsn)
}

func init() {
	BinlogCmd.AddCommand(BinlogSyncS3Cmd)
	BinlogCmd.AddCommand(BinlogRestoreCmd)
}

var BinlogCmd = &cobra.Command{
	Use:   "binlog",
	Short: "Manage MySQL binlog operations",
	Long:  "Commands for managing MySQL binlog operations.",
}

package cmd

import (
	"github.com/liweiyi88/onedump/binlog"
	"github.com/spf13/cobra"
)

var binlogCmd = &cobra.Command{
	Use:   "binlog",
	Short: "Database transaction log parser",
	Long:  "Database transaction log parser, it supports MySQL binlog",
	RunE: func(cmd *cobra.Command, args []string) error {
		parser := binlog.NewBinlogParser()
		err := parser.ParseFile("/opt/homebrew/var/mysql/binlog.000295")

		return err
	},
}

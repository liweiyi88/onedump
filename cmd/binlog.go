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
		err := binlog.ParseFile("/Users/julian.li/Downloads/binlog.002937")

		return err
	},
}

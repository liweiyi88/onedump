package cmd

import "github.com/spf13/cobra"

var binlogCmd = &cobra.Command{
	Use:   "binlog",
	Short: "Manage MySQL binlog operations",
	Long:  "Commands for managing MySQL binlog operations.",
}

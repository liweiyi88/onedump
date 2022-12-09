package cmd

import (
	"log"
	"strings"

	"github.com/liweiyi88/onedump/dump"
	"github.com/spf13/cobra"
)

var (
	dsn       string
	options   []string
	mysqlGzip bool
)

var mysqlDumpCmd = &cobra.Command{
	Use:   "mysql /path/to/dump-file.sql",
	Args:  cobra.ExactArgs(1),
	Short: "Dump mysql database to a file",
	Run: func(cmd *cobra.Command, args []string) {
		dumpFile := strings.TrimSpace(args[0])
		if dumpFile == "" {
			log.Fatal("you must specify the dump file path. e.g. /download/dump.sql")
		}

		job := &dump.Job{
			Name:     "mysql dump via cli",
			DBDriver: "mysql",
			DumpFile: dumpFile,
			DBDsn:    dsn,
			Options:  options,
			Gzip:     mysqlGzip,
		}

		job.Run().Print()
	},
}

func init() {
	rootCmd.AddCommand(mysqlDumpCmd)
	mysqlDumpCmd.Flags().StringVarP(&dsn, "dsn", "d", "", "database dsn (required) ")
	mysqlDumpCmd.MarkFlagRequired("dsn")
	mysqlDumpCmd.Flags().StringArrayVarP(&options, "options", "o", nil, "use options to overwrite the default or add new mysqldump options e.g. --dump-options \"--no-create-info\" --dump-options \"--skip-comments\"")
	mysqlDumpCmd.Flags().BoolVarP(&mysqlGzip, "gzip", "", true, "if need to gzip the file")
}

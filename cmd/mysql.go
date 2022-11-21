package cmd

import (
	"log"
	"strings"

	"github.com/liweiyi88/godump/dbdump"
	"github.com/spf13/cobra"
)

var (
	mysqldbName, mysqlUsername, mysqlPassword, mysqlHost string
	mysqlPort                                            int
	options                                              []string
)

var mysqlDumpCmd = &cobra.Command{
	Use:   "mysql",
	Args:  cobra.ExactArgs(1),
	Short: "Dump mysql database to a file",
	Run: func(cmd *cobra.Command, args []string) {
		dumpFile := strings.TrimSpace(args[0])
		if dumpFile == "" {
			log.Fatal("you must specify the dump file path. e.g. /download/dump.sql")
		}

		dumper := dbdump.NewMysqlDumper(mysqldbName, mysqlUsername, mysqlPassword, mysqlHost, mysqlPort, options, false)

		err := dumper.Dump(dumpFile)
		if err != nil {
			log.Fatal("failed to dump mysql datbase", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(mysqlDumpCmd)
	mysqlDumpCmd.Flags().StringVarP(&mysqldbName, "dbname", "d", "", "database name (required) ")
	mysqlDumpCmd.MarkFlagRequired("dbname")
	mysqlDumpCmd.Flags().StringArrayVarP(&options, "options", "o", nil, "use options to overwrite or add new mysqldump options")
	mysqlDumpCmd.Flags().StringVarP(&mysqlUsername, "user", "u", "root", "database username")
	mysqlDumpCmd.Flags().StringVarP(&mysqlPassword, "password", "p", "", "database password")
	mysqlDumpCmd.Flags().StringVar(&mysqlHost, "host", "127.0.0.1", "database host")
	mysqlDumpCmd.Flags().IntVar(&mysqlPort, "port", 3306, "database port")
}

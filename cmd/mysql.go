package cmd

import (
	"log"
	"strings"

	"github.com/liweiyi88/godump/dbdump"
	"github.com/spf13/cobra"
)

var (
	dbName, username, password, host string
	port int
	createTables bool
)

var mysqlDumpCmd = &cobra.Command{
	Use: "mysql",
	Args: cobra.ExactArgs(1),
	Short: "Dump mysql database to a file",
	Run: func(cmd *cobra.Command, args []string) {
		dumpFile := strings.TrimSpace(args[0])
		if dumpFile == "" {
			log.Fatal("you must specify the dump file path. e.g. /download/dump.sql")
		}

		dumper := dbdump.NewMysqlDumper()
		dumper.DBName = dbName
		dumper.Username = username
		dumper.Password = password
		dumper.Host = host
		dumper.Port = port
		dumper.CreateTables = createTables

		err := dumper.Dump(dumpFile)
		if err != nil {
			log.Fatal("failed to dump mysql datbase", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(mysqlDumpCmd)
	mysqlDumpCmd.Flags().StringVarP(&dbName, "dbname", "d", "", "database name")
	mysqlDumpCmd.MarkFlagRequired("dbname")
	mysqlDumpCmd.Flags().StringVarP(&username, "user", "u", "root", "database username")
	mysqlDumpCmd.Flags().StringVarP(&password, "password", "p", "", "database password")
	mysqlDumpCmd.Flags().StringVar(&host, "host", "127.0.0.1", "database host")
	mysqlDumpCmd.Flags().IntVar(&port, "port", 3306, "database port")
	mysqlDumpCmd.Flags().BoolVar(&createTables, "create-tables", true, "if set false, do not write CREATE TABLE statements that re-create each dumped table")
}
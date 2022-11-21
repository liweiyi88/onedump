package cmd

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/liweiyi88/godump/dbdump"
	"github.com/spf13/cobra"
)

var (
	sshHost, sshUser, sshPrivateKeyFile, databaseDsn string
	sshPort                                          int
	dumpOptions                                      []string
)

func getDumpCommand(dbDriver, dsn, dumpFile string, dumpOptions []string) (string, error) {
	switch dbDriver {
	case "mysql":
		config, err := mysql.ParseDSN(dsn)
		if err != nil {
			return "", err
		}

		host, port, err := net.SplitHostPort(config.Addr)
		if err != nil {
			return "", err
		}

		dbPort, err := strconv.Atoi(port)
		if err != nil {
			return "", err
		}

		mysqlDumper := dbdump.NewMysqlDumper(config.DBName, config.User, config.Passwd, host, dbPort, dumpOptions, true)
		command, err := mysqlDumper.GetSshDumpCommand(dumpFile)

		if err != nil {
			return "", err
		}

		return command, nil
	default:
		return "", fmt.Errorf("%s is not a supported database vendor", dbDriver)
	}
}

var sshDumpCmd = &cobra.Command{
	Use:   "ssh",
	Args:  cobra.ExactArgs(2),
	Short: "Dump remote database to a file",
	Run: func(cmd *cobra.Command, args []string) {
		dumpFile := strings.TrimSpace(args[1])
		if dumpFile == "" {
			log.Fatal("you must specify the dump file path. e.g. /download/dump.sql")
		}

		dbDriver := strings.TrimSpace(args[0])

		fmt.Println(dumpOptions, "dump")
		command, err := getDumpCommand(dbDriver, databaseDsn, dumpFile, dumpOptions)
		if err != nil {
			log.Fatal("failed to get database dump command", err)
		}

		sshDumper := dbdump.NewSshDumper(sshHost, sshUser, sshPrivateKeyFile, sshPort)
		err = sshDumper.Dump(dumpFile, command)

		if err != nil {
			log.Fatal("failed to run dump command via ssh", err)
		}
	},
}

// @TODO: use sshHost instead of sshHost and sshPort. then use similar logic to make sure we have default 22 sshport.
// Check mysql.ensureHavePort func
func init() {
	rootCmd.AddCommand(sshDumpCmd)
	sshDumpCmd.Flags().StringVarP(&sshHost, "sshHost", "", "", "Ssh host name (required) ")
	sshDumpCmd.MarkFlagRequired("sshHost")
	sshDumpCmd.Flags().IntVarP(&sshPort, "sshPort", "", 22, "Ssh port")
	sshDumpCmd.Flags().StringVarP(&sshUser, "sshUser", "", "root", "ssh username")
	sshDumpCmd.Flags().StringVarP(&sshPrivateKeyFile, "privateKeyFile", "f", "", "private key file path for ssh connection")
	sshDumpCmd.Flags().StringArrayVarP(&dumpOptions, "dump-options", "", nil, "use options to overwrite or add new dump command options")
	sshDumpCmd.Flags().StringVarP(&databaseDsn, "dbDsn", "", "", "the database dsn for connection")
	sshDumpCmd.MarkFlagRequired("dbDsn")
}

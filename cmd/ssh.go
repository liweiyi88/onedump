package cmd

import (
	"fmt"
	"log"
	"strings"

	"github.com/liweiyi88/onedump/dbdump"
	"github.com/spf13/cobra"
)

var (
	sshHost, sshUser, sshPrivateKeyFile, databaseDsn string
	dumpOptions                                      []string
)

func getDumpCommand(dbDriver, dsn, dumpFile string, dumpOptions []string) (string, error) {
	switch dbDriver {
	case "mysql":
		mysqlDumper, err := dbdump.NewMysqlDumper(dsn, dumpOptions, true)
		if err != nil {
			return "", err
		}

		command, err := mysqlDumper.GetSshDumpCommand()

		if err != nil {
			return "", err
		}

		return command, nil
	default:
		return "", fmt.Errorf("%s is not a supported database driver", dbDriver)
	}
}

var sshDumpCmd = &cobra.Command{
	Use:   "ssh mysql </path/to/dump-file.sql>",
	Args:  cobra.ExactArgs(2),
	Short: "Dump remote database to a file",
	Run: func(cmd *cobra.Command, args []string) {
		dumpFile := strings.TrimSpace(args[1])
		if dumpFile == "" {
			log.Fatal("you must specify the dump file path. e.g. /download/dump.sql")
		}

		dbDriver := strings.TrimSpace(args[0])

		command, err := getDumpCommand(dbDriver, databaseDsn, dumpFile, dumpOptions)
		if err != nil {
			log.Fatal("failed to get database dump command", err)
		}

		sshDumper := dbdump.NewSshDumper(sshHost, sshUser, sshPrivateKeyFile)
		err = sshDumper.Dump(dumpFile, command)

		if err != nil {
			log.Fatal("failed to run dump command via ssh", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(sshDumpCmd)
	sshDumpCmd.Flags().StringVarP(&sshHost, "sshHost", "", "", "SSH host e.g. yourdomain.com (you can omit port as it uses 22 by default) or 56.09.139.09:33. (required) ")
	sshDumpCmd.MarkFlagRequired("sshHost")
	sshDumpCmd.Flags().StringVarP(&sshUser, "sshUser", "", "root", "SSH username")
	sshDumpCmd.Flags().StringVarP(&sshPrivateKeyFile, "privateKeyFile", "f", "", "private key file path for SSH connection")
	sshDumpCmd.Flags().StringArrayVarP(&dumpOptions, "dump-options", "", nil, "use options to overwrite or add new dump command options. e.g. for mysql: --dump-options \"--no-create-info\" --dump-options \"--skip-comments\"")
	sshDumpCmd.Flags().StringVarP(&databaseDsn, "dbDsn", "", "", "the database dsn for connection. e.g. <dbUser>:<dbPass>@tcp(<dbHost>:<dbPort>)/<dbName>")
	sshDumpCmd.MarkFlagRequired("dbDsn")
}

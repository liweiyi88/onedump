package cmd

import (
	"log"
	"strings"

	"github.com/liweiyi88/godump/dbdump"
	"github.com/spf13/cobra"
)

var (
	sshHost, sshUser, sshPrivateKeyFile string
	sshPort int
)


var sshDumpCmd = &cobra.Command{
	Use: "mysql",
	Args: cobra.ExactArgs(1),
	Short: "Dump mysql database to a file",
	Run: func(cmd *cobra.Command, args []string) {
		dumpFile := strings.TrimSpace(args[0])

		if dumpFile == "" {
			log.Fatal("you must specify the dump file path. e.g. /download/dump.sql")
		}

		sshDumper := dbdump.NewSshDumper()
		sshDumper.Host = sshHost
		sshDumper.User = sshUser
		sshDumper.PrivateKeyFile = sshPrivateKeyFile

		err := sshDumper.Dump(dumpFile)
		if err != nil {
			log.Fatal("failed to ssh dump datbase", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(sshDumpCmd)
	sshDumpCmd.Flags().StringVarP(&sshHost, "sshHost", "d", "", "Ssh host name (required) ")
	sshDumpCmd.MarkFlagRequired("sshHost")
	sshDumpCmd.Flags().IntVarP(&sshPort, "sshPort", "d", 22, "Ssh port")
	sshDumpCmd.MarkFlagRequired("sshPort")
	sshDumpCmd.Flags().StringVarP(&sshUser, "user", "u", "root", "ssh username")
	sshDumpCmd.Flags().StringVarP(&sshPrivateKeyFile, "privateKeyFile", "f", "", "private key file path for ssh connection")
	sshDumpCmd.Flags().BoolVar(&createTables, "create-tables", true, "if set false, do not write CREATE TABLE statements that re-create each dumped table")
}
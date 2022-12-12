package cmd

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/liweiyi88/onedump/dump"
	"github.com/spf13/cobra"
)

var (
	sshHost, sshUser, sshPrivateKeyFile string
	dbDsn                               string
	jobName                             string
	dumpOptions                         []string
	gzip                                bool
)

var rootCmd = &cobra.Command{
	Use:   "<dbdriver> </path/to/dump-file.sql>",
	Short: "Dump database content from a source to a destination.",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		driver := strings.TrimSpace(args[0])

		dumpFile := strings.TrimSpace(args[1])
		if dumpFile == "" {
			log.Fatal("you must specify the dump file path. e.g. /download/dump.sql")
		}

		job := dump.NewJob(
			"dump via cli",
			driver,
			dumpFile,
			dbDsn,
			dump.WithGzip(gzip),
			dump.WithSshHost(sshHost),
			dump.WithSshUser(sshUser),
			dump.WithPrivateKeyFile(sshPrivateKeyFile),
			dump.WithDumpOptions(dumpOptions),
		)

		job.Run().Print()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVarP(&sshHost, "ssh-host", "", "", "SSH host e.g. yourdomain.com (you can omit port as it uses 22 by default) or 56.09.139.09:33. (required) ")
	rootCmd.Flags().StringVarP(&sshUser, "ssh-user", "", "root", "SSH username")
	rootCmd.Flags().StringVarP(&sshPrivateKeyFile, "privatekey", "f", "", "private key file path for SSH connection")
	rootCmd.Flags().StringArrayVarP(&dumpOptions, "dump-options", "", nil, "use options to overwrite or add new dump command options. e.g. for mysql: --dump-options \"--no-create-info\" --dump-options \"--skip-comments\"")
	rootCmd.Flags().StringVarP(&dbDsn, "db-dsn", "d", "", "the database dsn for connection. e.g. <dbUser>:<dbPass>@tcp(<dbHost>:<dbPort>)/<dbName>")
	rootCmd.MarkFlagRequired("db-dsn")
	rootCmd.Flags().BoolVarP(&gzip, "gzip", "g", true, "if need to gzip the file")
	rootCmd.Flags().StringVarP(&jobName, "job-name", "", "", "The dump job name")
}

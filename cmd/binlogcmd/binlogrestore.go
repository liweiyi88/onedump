package binlogcmd

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/liweiyi88/onedump/binlog"
	"github.com/liweiyi88/onedump/env"
	"github.com/liweiyi88/onedump/storage/s3"
	"github.com/spf13/cobra"
)

var (
	dir, mysqlbinlogPath, stopDateTime, startBinlog, dumpFilePath string
	startPosition                                                 int
)

func init() {
	BinlogRestoreS3Cmd.Flags().StringVarP(&s3Bucket, "s3-bucket", "b", "", "AWS S3 bucket name that used for saving binlog files (required)")
	BinlogRestoreS3Cmd.Flags().StringVarP(&s3Prefix, "s3-prefix", "p", "", "AWS S3 file prefix (folder) that used for saving binlog files (required)")
	BinlogRestoreS3Cmd.Flags().StringVarP(&dir, "dir", "d", "", "A directory that saves binlog files temporally (required)")
	BinlogRestoreS3Cmd.Flags().StringVar(&mysqlbinlogPath, "mysqlbinlog-path", "mysqlbinlog", "Set the mysqlbinlog command path, default: mysqlbinlog (optional)")
	BinlogRestoreS3Cmd.Flags().StringVar(&stopDateTime, "stop-datetime", time.Now().Format(time.DateTime), "Set the stop datetime for point-in-time recovery. Defaults to the current time. (optional)")
	BinlogRestoreS3Cmd.Flags().StringVar(&startBinlog, "start-binlog", "", "Binlog file to start recovery from (optional if --dump-file is provided)")
	BinlogRestoreS3Cmd.Flags().IntVar(&startPosition, "start-position", 0, "Position in the binlog file to begin recovery (optional if --dump-file is provided)")
	BinlogRestoreS3Cmd.Flags().StringVar(&dumpFilePath, "dump-file", "", "Full database dump that contains binlog file and position (optional if --start-binlog and --start-position are provided)")
	BinlogRestoreS3Cmd.Flags().BoolVar(&dryRun, "dry-run", false, "If true, output only the SQL restore statements without applying them. default: false (optional)")
	BinlogRestoreS3Cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "prints additional debug information (optional)")
	BinlogRestoreS3Cmd.MarkFlagRequired("dir")
	BinlogRestoreS3Cmd.MarkFlagRequired("s3-bucket")
	BinlogRestoreS3Cmd.MarkFlagRequired("s3-prefix")
	BinlogRestoreS3Cmd.MarkFlagsRequiredTogether("start-binlog", "start-position")
	BinlogRestoreS3Cmd.MarkFlagsOneRequired("start-binlog", "dump-file")
}

var BinlogRestoreS3Cmd = &cobra.Command{
	Use:   "restore s3",
	Short: "Restore database from MySQL binlogs stored in an AWS S3 bucket",
	Long: `Restore database from MySQL binlogs stored in an AWS S3 bucket
It requires the following environment variables:
  - AWS_REGION
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
  - DATABASE_DSN // e.g. root@tcp(127.0.0.1)/

  AWS_SESSION_TOKEN is optional unless you use a temporary credentials
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		requireEnvVars := []string{AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DATABASE_DSN}

		if err := env.ValidateEnvVars(requireEnvVars); err != nil {
			return err
		}

		if verbose {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		dsn := os.Getenv(DATABASE_DSN)
		region := os.Getenv(AWS_REGION)
		accessKeyId := os.Getenv(AWS_ACCESS_KEY_ID)
		secretAccessKey := os.Getenv(AWS_SECRET_ACCESS_KEY)
		sessionToken := os.Getenv(AWS_SESSION_TOKEN)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return fmt.Errorf("fail to open database, error: %v", err)
		}

		defer func() {
			if err := db.Close(); err != nil {
				slog.Error("fail to close DB", slog.Any("error", err))
			}
		}()

		if err := db.Ping(); err != nil {
			return fmt.Errorf("fail to connect to database, error: %v", err)
		}

		if strings.TrimSpace(dumpFilePath) != "" {
			dumpFile, err := os.Open(dumpFilePath)

			if err != nil {
				return fmt.Errorf("fail to open dump file: %s, error: %v", dumpFilePath, err)
			}

			defer func() {
				if err := dumpFile.Close(); err != nil {
					slog.Error("fail to close dump file", slog.Any("dumpFile", dumpFilePath), slog.Any("error", err))
				}
			}()

			file, pos, err := binlog.ParseBinlogFilePosition(dumpFile)
			if err != nil {
				return fmt.Errorf("fail to parse binlog file position from %s, error: %v", dumpFilePath, err)
			}

			startBinlog = file
			startPosition = pos
		}

		binlogRestorer := binlog.NewBinlogRestorer(
			dir,
			startBinlog,
			startPosition,
			binlog.WithMySQLBinlogPath(mysqlbinlogPath),
			binlog.WithDryRun(dryRun),
			binlog.WithStopDateTime(stopDateTime),
		)

		if err := binlogRestorer.ValidateExternalCommandPaths(); err != nil {
			return err
		}

		ctx := context.Background()
		s3 := s3.NewS3(s3Bucket, "", region, accessKeyId, secretAccessKey, sessionToken)
		err = s3.DownloadObjects(ctx, s3Prefix, dir)
		if err != nil {
			return err
		}

		return binlogRestorer.Restore()
	},
}

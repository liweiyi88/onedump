package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/liweiyi88/onedump/binlog"
	"github.com/liweiyi88/onedump/storage/s3"
	"github.com/spf13/cobra"
)

var binlogRestoreS3Cmd = &cobra.Command{
	Use:   "restore-s3",
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

		if err := validateEnvVars(requireEnvVars); err != nil {
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

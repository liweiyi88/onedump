package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/liweiyi88/onedump/binlog"
	"github.com/liweiyi88/onedump/storage/s3"
	"github.com/spf13/cobra"
)

func validateEnvVars(vars []string) error {
	var errs error

	for _, v := range vars {
		if os.Getenv(v) == "" {
			errs = errors.Join(errs, fmt.Errorf("missing required environment variable %s", v))
		}
	}

	return errs
}

const (
	AWS_REGION            = "AWS_REGION"
	AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
	DATABASE_DSN          = "DATABASE_DSN"

	BINLOG_TOGGLE_QUERY = "SHOW VARIABLES LIKE 'log_bin';"
)

var binlogSyncS3Cmd = &cobra.Command{
	Use:   "binlog sync-s3",
	Short: "Sync local MySQL binlog files to an AWS S3 bucket",
	Long: `Sync local Mysql binlog files to an AWS S3 bucket.
It requires the following environment variables:
  - AWS_REGION
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
  - DATABASE_DSN // e.g. root@tcp(127.0.0.1)/
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

		querier := binlog.NewBinlogInfoQuerier(db)
		binlogInfo, err := querier.GetBinlogInfo()

		if err != nil {
			return fmt.Errorf("fail to create binlog info querier, error: %v", err)
		}

		s3 := s3.NewS3(s3Bucket, "", region, accessKeyId, secretAccessKey)
		syncer := binlog.NewBinlogSyncer(s3Prefix, checksum, binlogInfo)
		return syncer.Sync(s3)
	},
}

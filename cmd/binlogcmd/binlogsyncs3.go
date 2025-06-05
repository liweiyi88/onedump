package binlogcmd

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/liweiyi88/onedump/binlog"
	"github.com/liweiyi88/onedump/env"
	"github.com/liweiyi88/onedump/filesync"
	"github.com/liweiyi88/onedump/storage/s3"
	"github.com/spf13/cobra"
)

const (
	AWS_REGION            = "AWS_REGION"
	AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
	AWS_SESSION_TOKEN     = "AWS_SESSION_TOKEN"
	DATABASE_DSN          = "DATABASE_DSN"

	BINLOG_TOGGLE_QUERY = "SHOW VARIABLES LIKE 'log_bin';"
)

var (
	checksumFile, logFile string
	checksum, saveLog     bool
)

func init() {
	// The command also needs the DATABASE_URL, AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY as env var
	BinlogSyncS3Cmd.Flags().StringVarP(&s3Bucket, "s3-bucket", "b", "", "AWS S3 bucket name that used for saving binlog files (required)")
	BinlogSyncS3Cmd.Flags().StringVarP(&s3Prefix, "s3-prefix", "p", "", "AWS S3 file prefix (folder) that used for saving binlog files (required)")
	BinlogSyncS3Cmd.MarkFlagRequired("s3-bucket")
	BinlogSyncS3Cmd.MarkFlagRequired("s3-prefix")
	BinlogSyncS3Cmd.Flags().BoolVar(&checksum, "checksum", false, "whether to save the checksum to avoid repeating file transfers, default: false (optional)")
	BinlogSyncS3Cmd.Flags().StringVar(&checksumFile, "checksum-file", "", "save checksum results in a specific file if --checksum=true, default: /path/to/sync/folder/checksum.onedump (optional)")
	BinlogSyncS3Cmd.Flags().BoolVar(&saveLog, "save-log", false, "whether to save the sync results in a log file, default: false (optional)")
	BinlogSyncS3Cmd.Flags().StringVar(&logFile, "log-file", "", "save result log in a specific file if --save-log=true. default: /path/to/binlogs/onedump-binlog-sync.log (optional)")
	BinlogSyncS3Cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "prints additional debug information (optional)")
}

var BinlogSyncS3Cmd = &cobra.Command{
	Use:   "sync s3",
	Short: "Sync local MySQL binlog files to an AWS S3 bucket",
	Long: `Sync local Mysql binlog files to an AWS S3 bucket.
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

		querier := binlog.NewBinlogInfoQuerier(db)
		binlogInfo, err := querier.GetBinlogInfo()

		if err != nil {
			return fmt.Errorf("fail to create binlog info querier, error: %v", err)
		}

		s3 := s3.NewS3(s3Bucket, "", region, accessKeyId, secretAccessKey, sessionToken)
		fs := filesync.NewFileSync(checksum, checksumFile)
		syncer := binlog.NewBinlogSyncer(s3Prefix, saveLog, logFile, fs, binlogInfo)
		return syncer.Sync(s3)
	},
}

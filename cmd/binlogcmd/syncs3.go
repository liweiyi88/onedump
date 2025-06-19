package binlogcmd

import (
	"fmt"
	"log/slog"

	"github.com/liweiyi88/onedump/binlog"
	"github.com/liweiyi88/onedump/env"
	"github.com/liweiyi88/onedump/filesync"
	"github.com/liweiyi88/onedump/storage/s3"
	"github.com/spf13/cobra"
)

var (
	checksumFile, logFile string
	checksum, saveLog     bool
)

func init() {
	// The command also needs the DATABASE_URL, AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY as environment variables.
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
		envs, err := env.NewEnvResolver(
			env.WithAWS(),
			env.WithDatabaseDSN()).
			Resolve()

		if err != nil {
			return err
		}

		if verbose {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		db, err := OpenDB(envs.DatabaseDSN)
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

		querier := binlog.NewBinlogQuerier(db)
		binlogInfo, err := querier.GetBinlogInfo()

		if err != nil {
			return fmt.Errorf("fail to get binlog info, error: %v", err)
		}

		credentials := envs.AWSCredentials
		s3 := s3.NewS3(
			s3Bucket,
			"",
			credentials.Region,
			credentials.AccessKeyID,
			credentials.SecretAccessKey,
			credentials.SessionToken)

		fs := filesync.NewFileSync(checksum, checksumFile)
		syncer := binlog.NewBinlogSyncer(s3Prefix, saveLog, logFile, fs, binlogInfo)
		return syncer.Sync(s3)
	},
}

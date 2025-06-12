package binlogcmd

import (
	"compress/gzip"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/liweiyi88/onedump/binlog"
	"github.com/liweiyi88/onedump/env"
	"github.com/liweiyi88/onedump/fileutil"
	"github.com/spf13/cobra"
)

var (
	dir, mysqlbinlogPath, stopDateTime, startBinlog, dumpFilePath string
	startPosition                                                 int
)

func init() {
	BinlogRestoreS3Cmd.Flags().StringVarP(&dir, "dir", "d", "", "A directory that saves binlog files temporally (required)")
	BinlogRestoreS3Cmd.Flags().StringVar(&mysqlbinlogPath, "mysqlbinlog-path", "mysqlbinlog", "Set the mysqlbinlog command path, default: mysqlbinlog (optional)")
	BinlogRestoreS3Cmd.Flags().StringVar(&stopDateTime, "stop-datetime", time.Now().Format(time.DateTime), "Set the stop datetime for point-in-time recovery. Defaults to the current time. (optional)")
	BinlogRestoreS3Cmd.Flags().StringVar(&startBinlog, "start-binlog", "", "Binlog file to start recovery from (optional if --dump-file is provided)")
	BinlogRestoreS3Cmd.Flags().IntVar(&startPosition, "start-position", 0, "Position in the binlog file to begin recovery (optional if --dump-file is provided)")
	BinlogRestoreS3Cmd.Flags().StringVar(&dumpFilePath, "dump-file", "", "A Database dump file that contains binlog file and position (optional if --start-binlog and --start-position are provided)")
	BinlogRestoreS3Cmd.Flags().BoolVar(&dryRun, "dry-run", false, "If true, output only the SQL restore statements without applying them. default: false (optional)")
	BinlogRestoreS3Cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "prints additional debug information (optional)")
	BinlogRestoreS3Cmd.MarkFlagRequired("dir")
	BinlogRestoreS3Cmd.MarkFlagsRequiredTogether("start-binlog", "start-position")
	BinlogRestoreS3Cmd.MarkFlagsOneRequired("start-binlog", "dump-file")
}

var BinlogRestoreS3Cmd = &cobra.Command{
	Use:   "restore s3",
	Short: "Restore database from MySQL binlogs that are saved in an AWS S3 bucket",
	Long: `Restore database from MySQL binlogs that are saved in an AWS S3 bucket
It requires the following environment variables:
  - DATABASE_DSN // e.g. root@tcp(127.0.0.1)/
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		envs, err := env.NewEnvResolver(env.WithDatabaseDSN()).Resolve()

		if verbose {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		if err != nil {
			return err
		}

		db, err := sql.Open("mysql", envs.DatabaseDSN)
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
			file, pos, err := extractBinlogStartFilePosition(dumpFilePath)
			if err != nil {
				return fmt.Errorf("fail to extract binlog and position from dump file: %s, error: %v", dumpFilePath, err)
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
			binlog.WithDatabaseDSN(envs.DatabaseDSN),
		)

		if err := binlogRestorer.EnsureMysqlCommandPaths(); err != nil {
			return err
		}

		return binlogRestorer.Restore()
	},
}

func extractBinlogStartFilePosition(filePath string) (string, int, error) {
	var dumpReader io.Reader
	dumpFile, err := os.Open(filePath)

	if err != nil {
		return "", 0, fmt.Errorf("fail to open dump file: %s, error: %v", filePath, err)
	}

	defer func() {
		if err := dumpFile.Close(); err != nil {
			slog.Error("fail to close dump file", slog.Any("dumpFile", filePath), slog.Any("error", err))
		}
	}()

	if fileutil.IsGzipped(filePath) {
		gzipReader, err := gzip.NewReader(dumpFile)
		if err != nil {
			return "", 0, fmt.Errorf("fail to create a gzip reader, error: %v", err)
		}

		defer func() {
			if err := gzipReader.Close(); err != nil {
				slog.Error("fail to close gzip reader when parse slow log", slog.Any("error", err))
			}
		}()

		dumpReader = gzipReader
	} else {
		dumpReader = dumpFile
	}

	file, pos, err := binlog.ParseBinlogFilePosition(dumpReader)
	if err != nil {
		return "", 0, fmt.Errorf("fail to parse binlog file position from %s, error: %v", dumpFilePath, err)
	}

	return file, pos, nil
}

package binlogcmd

import (
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

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
	BinlogRestoreCmd.Flags().StringVarP(&dir, "dir", "d", "", "A directory that saves binlog files temporally (required)")
	BinlogRestoreCmd.Flags().StringVar(&mysqlbinlogPath, "mysqlbinlog-path", "mysqlbinlog", "Set the mysqlbinlog command path, default: mysqlbinlog (optional)")
	BinlogRestoreCmd.Flags().StringVar(&stopDateTime, "stop-datetime", "", "Set the stop datetime for point-in-time recovery. Defaults to the current time. (optional)")
	BinlogRestoreCmd.Flags().StringVar(&startBinlog, "start-binlog", "", "Binlog file to start recovery from (optional if --dump-file is provided)")
	BinlogRestoreCmd.Flags().IntVar(&startPosition, "start-position", 0, "Position in the binlog file to begin recovery (optional if --dump-file is provided)")
	BinlogRestoreCmd.Flags().StringVar(&dumpFilePath, "dump-file", "", "A Database dump file that contains binlog file and position (optional if --start-binlog and --start-position are provided)")
	BinlogRestoreCmd.Flags().BoolVar(&dryRun, "dry-run", false, "If true, output the parsed binlog events instead of applying them. default: false (optional)")
	BinlogRestoreCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "prints additional debug information (optional)")
	BinlogRestoreCmd.MarkFlagRequired("dir")
	BinlogRestoreCmd.MarkFlagsRequiredTogether("start-binlog", "start-position")
	BinlogRestoreCmd.MarkFlagsOneRequired("start-binlog", "dump-file")
}

var BinlogRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore the database from MySQL binlogs",
	Long: `Restore the database from MySQL binlogs
It requires the following environment variables:
  - DATABASE_DSN // e.g. root@tcp(127.0.0.1)/
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		envs, err := env.NewEnvResolver(env.WithDatabaseDSN()).Resolve()
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
			slog.Error("fail to close dump file", slog.String("dumpFile", filePath), slog.Any("error", err))
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

package binlog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"
	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/sliceutil"
)

const (
	DefaultMysqlPath       = "mysql"
	DefaultMySQLBinlogPath = "mysqlbinlog"
	MaxBinlogsPerExecution = 10 // limit to avoid exceeding the OS argument size when calling mysqlbinlog
)

var (
	// A fake error to stop parsing events.
	ErrEventBeforeStopDatetime = errors.New("event is before stop datetime")
	ErrStopPositionNotFound    = errors.New("stop position not found")
	ErrBinlogsNotFound         = errors.New("no binlog files were found in the directory")
)

type binlogRestorePlan struct {
	startPosition int
	stopPosition  *int
	binlogs       []string
}

func newBinlogRestorePlan(startPosition int) *binlogRestorePlan {
	return &binlogRestorePlan{
		startPosition: startPosition,
		binlogs:       make([]string, 0),
	}
}

type BinlogRestorer struct {
	binlogDir       string
	dryRun          bool
	dsn             string
	mysqlPath       string
	mysqlbinlogPath string
	startBinlog     string
	startPosition   int
	stopDateTime    time.Time
}

func NewBinlogRestorer(binlogDir string, startBinlog string, startPosition int, opts ...binlogRestoreOption) *BinlogRestorer {
	binlogRestorer := &BinlogRestorer{
		startBinlog:     startBinlog,
		startPosition:   startPosition,
		binlogDir:       binlogDir,
		mysqlPath:       DefaultMysqlPath,
		mysqlbinlogPath: DefaultMySQLBinlogPath,
	}

	for _, opt := range opts {
		opt(binlogRestorer)
	}

	return binlogRestorer
}

type binlogRestoreOption func(binlogRestorer *BinlogRestorer)

func WithMySQLPath(mysqlPath string) binlogRestoreOption {
	return func(binlogRestorer *BinlogRestorer) {
		binlogRestorer.mysqlPath = mysqlPath
	}
}

func WithMySQLBinlogPath(mysqlbinlogPath string) binlogRestoreOption {
	return func(binlogRestorer *BinlogRestorer) {
		binlogRestorer.mysqlbinlogPath = mysqlbinlogPath
	}
}

func WithDryRun(dryRun bool) binlogRestoreOption {
	return func(binlogRestorer *BinlogRestorer) {
		binlogRestorer.dryRun = dryRun
	}
}

func WithStopDateTime(stopDateTime string) binlogRestoreOption {
	return func(binlogRestorer *BinlogRestorer) {
		time, err := time.Parse(time.DateTime, stopDateTime)
		if err != nil {
			panic(fmt.Sprintf("invalid value of --stop-datetime option, error: %v", err))
		}

		binlogRestorer.stopDateTime = time
	}
}

func WithDatabaseDSN(dsn string) binlogRestoreOption {
	return func(binlogRestorer *BinlogRestorer) {
		binlogRestorer.dsn = dsn
	}
}

func (b *BinlogRestorer) EnsureMySQLCommandPaths() error {
	if _, err := exec.LookPath(b.mysqlbinlogPath); err != nil {
		return fmt.Errorf("%s command is required but not found: %v", "mysqlbinlog", err)
	}

	if !b.dryRun {
		if _, err := exec.LookPath(b.mysqlPath); err != nil {
			return fmt.Errorf("%s command is required but not found: %v", "mysql", err)
		}
	}

	return nil
}

func extractBinlogNumber(filename string) int64 {
	parts := strings.Split(filename, ".")

	if len(parts) != 2 {
		return 0
	}

	numStr := parts[len(parts)-1]

	num, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0
	}

	return num
}

func (b *BinlogRestorer) getSortedBinlogs() ([]string, error) {
	binlogs, err := fileutil.ListFiles(b.binlogDir, "", ".index")
	if err != nil {
		return nil, fmt.Errorf("fail to list binlog files from %s, error: %v", b.binlogDir, err)
	}

	if len(binlogs) == 0 {
		return nil, ErrBinlogsNotFound
	}

	sort.Slice(binlogs, func(i, j int) bool {
		a := extractBinlogNumber(binlogs[i])
		b := extractBinlogNumber(binlogs[j])
		return a < b
	})

	startIndex := -1

	for i, name := range binlogs {
		if filepath.Base(name) == b.startBinlog {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		return nil, fmt.Errorf("current binlog %s not found", b.startBinlog)
	}

	return binlogs[startIndex:], nil
}

func (b *BinlogRestorer) createBinlogRestorePlan() (*binlogRestorePlan, error) {
	binlogs, err := b.getSortedBinlogs()
	if err != nil {
		return nil, err
	}

	parser := replication.NewBinlogParser()
	plan := newBinlogRestorePlan(b.startPosition)

	for _, binlog := range binlogs {
		plan.binlogs = append(plan.binlogs, binlog)

		err := parser.ParseFile(binlog, 0, func(e *replication.BinlogEvent) error {
			eventTime := time.Unix(int64(e.Header.Timestamp), 0)
			pos := e.Header.LogPos

			slog.Debug("parsing binlog file...",
				slog.Any("binlog file", binlog),
				slog.Any("stop datetime", b.stopDateTime),
				slog.Any("event time", eventTime),
				slog.Any("log position", pos),
			)

			// The --stop-datetime option is exclusive in mysqlbinlog command
			// So lets keep the logic consistent with mysqlbinlog
			if !eventTime.Before(b.stopDateTime) {
				return ErrEventBeforeStopDatetime
			}

			stopPos := int(e.Header.LogPos)
			plan.stopPosition = &stopPos
			return nil
		})

		if err != nil {
			if !errors.Is(err, ErrEventBeforeStopDatetime) {
				return nil, fmt.Errorf("fail to parse binlog file: %s: %v", binlog, err)
			}

			// If we cannot find the stop position in this binlog,
			// then let's continue to try to find the stop position in the next binlog file.
			if plan.stopPosition == nil {
				continue
			}

			return plan, nil
		}
	}

	// We should find a stop position.
	// If we can't find the position, it means the value of stop datetime is before all events from all binlog files.
	if plan.stopPosition == nil {
		return nil, ErrStopPositionNotFound
	}

	return plan, nil
}

func (b *BinlogRestorer) createRestoreCommandArgs(plan *binlogRestorePlan) []string {
	args := make([]string, 0)

	if len(plan.binlogs) == 0 {
		slog.Debug("no binlog file is included in restore plan, skip")
		return args
	}

	if len(plan.binlogs) == 1 && plan.stopPosition != nil && plan.startPosition == *plan.stopPosition {
		slog.Debug("start position is the same as the stop position, skip")
		return args
	}

	if len(plan.binlogs) == 1 {
		var command string
		binlog := plan.binlogs[0]
		if plan.stopPosition != nil {
			command = fmt.Sprintf("%s --start-position=%d --stop-position=%d", binlog, plan.startPosition, *plan.stopPosition)
		} else {
			command = fmt.Sprintf("%s --start-position=%d", binlog, plan.startPosition)
		}

		args = append(args, command)
		return args
	}

	firstBinlog := plan.binlogs[0]
	firstCommand := fmt.Sprintf("%s --start-position=%d", firstBinlog, plan.startPosition)
	args = append(args, firstCommand)

	middleBinlogs := plan.binlogs[1 : len(plan.binlogs)-1]

	if len(middleBinlogs) > 0 {
		chunkBinlogs := sliceutil.Chunk(middleBinlogs, MaxBinlogsPerExecution)
		for _, binlogs := range chunkBinlogs {
			command := fmt.Sprintf("%s", strings.Join(binlogs, " "))
			args = append(args, command)
		}
	}

	lastBinlog := plan.binlogs[len(plan.binlogs)-1]
	if plan.stopPosition != nil {
		args = append(args, fmt.Sprintf("%s --stop-position=%d", lastBinlog, *plan.stopPosition))
	} else {
		args = append(args, lastBinlog)
	}

	return args
}

// Use mysqlbinlog to restore data -> if --dry-run just output the content, otherwise pipe it with mysql
func (b *BinlogRestorer) Restore() error {
	plan, err := b.createBinlogRestorePlan()
	if err != nil {
		return fmt.Errorf("fail to create binlog restore plan, error: %v", err)
	}

	cmdArgs := b.createRestoreCommandArgs(plan)

	for _, argsString := range cmdArgs {
		args := strings.Fields(argsString)
		mysqlBinlogCmd := exec.Command(b.mysqlbinlogPath, args...)
		mysqlBinlogCmd.Stderr = os.Stderr

		// echo the results to stdout and stderr if dry run
		if b.dryRun {
			mysqlBinlogCmd.Stdout = os.Stdout
			if err := mysqlBinlogCmd.Run(); err != nil {
				return fmt.Errorf("fail to run mysqlbinlog command with args: %s, error: %v", args, err)
			}
		} else {
			// pipe the results of mysqlbinlog command to mysql
			mysqlBinlogCmdOut, err := mysqlBinlogCmd.StdoutPipe()

			if err != nil {
				return fmt.Errorf("fail to get restore command std out pipe, error: %v", err)
			}

			cfg, err := mysql.ParseDSN(b.dsn)
			if err != nil {
				return fmt.Errorf("fail to parse database dsn: %s, error: %v", b.dsn, err)
			}

			mysqlCmd := exec.Command(b.mysqlPath, "-u", cfg.User, "-p", cfg.Passwd)
			mysqlCmd.Stdin = mysqlBinlogCmdOut
			mysqlCmd.Stdout = os.Stdout
			mysqlCmd.Stderr = os.Stderr

			if err := mysqlBinlogCmd.Start(); err != nil {
				return fmt.Errorf("fail to start mysqlbinlog command with args: %s, error: %v", args, err)
			}

			if err := mysqlCmd.Start(); err != nil {
				return fmt.Errorf("fail to start mysql command: %v", err)
			}

			if err := mysqlBinlogCmd.Wait(); err != nil {
				return fmt.Errorf("mysqlbinlog command with args: %s failed, error: %v", args, err)
			}

			if err := mysqlCmd.Wait(); err != nil {
				return fmt.Errorf("mysql command failed: %v", err)
			}
		}
	}

	return nil
}

// Extracts the binlog file and position from a database dump file.
// The dump file must be created using mysqldump with the --master-data=2 option.
func ParseBinlogFilePosition(reader io.Reader) (string, int, error) {
	scanner := bufio.NewScanner(reader)
	regex := regexp.MustCompile(`(?i)(MASTER|SOURCE)_LOG_FILE\s*=\s*'([^']+)'\s*,\s*(MASTER|SOURCE)_LOG_POS\s*=\s*(\d+)`)

	for scanner.Scan() {
		line := scanner.Text()
		matches := regex.FindStringSubmatch(line)

		if len(matches) == 5 {
			file := matches[2]
			position := matches[4]

			pos, err := strconv.Atoi(position)
			if err != nil {
				return "", 0, fmt.Errorf("invalid position value: %s, error: %v", position, err)
			}

			return file, pos, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", 0, errors.New("fail to scan while extracting binlog file and position.")
	}

	return "", 0, errors.New("could not extract binlog file and position.")
}

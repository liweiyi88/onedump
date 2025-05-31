package binlog

import (
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/liweiyi88/onedump/fileutil"
)

const (
	DefaultMysqlPath       = "mysql"
	DefaultMySQLBinlogPath = "mysqlbinlog"
)

var ErrStopPositionFound = errors.New("stop position found")

type binlogRestoreOption func(binlogRestorer *BinlogRestorer)

type binlogRestorePlan struct {
	firstBinlog   string
	startPosition int
	lastBinlog    string
	stopPosition  int
	binlogs       []string
}

type BinlogRestorer struct {
	binlogInfoQuerier *binlogInfoQuerier
	binlogDir         string
	dryRun            bool
	mysqlPath         string
	mysqlbinlogPath   string
	stopDateTime      time.Time
}

func NewBinlogRestorer(binlogInfoQuerier *binlogInfoQuerier, binlogDir string, opts ...binlogRestoreOption) *BinlogRestorer {
	binlogRestorer := &BinlogRestorer{
		binlogInfoQuerier: binlogInfoQuerier,
		binlogDir:         binlogDir,
		mysqlPath:         DefaultMysqlPath,
		mysqlbinlogPath:   DefaultMySQLBinlogPath,
	}

	for _, opt := range opts {
		opt(binlogRestorer)
	}

	return binlogRestorer
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

func (b *BinlogRestorer) ValidateExternalCommandPaths() error {
	if _, err := exec.LookPath(b.mysqlbinlogPath); err != nil {
		return fmt.Errorf("command %s not found: %v", "mysqlbinlog", err)
	}

	if !b.dryRun {
		if _, err := exec.LookPath(b.mysqlPath); err != nil {
			return fmt.Errorf("command %s not found: %v", "mysql", err)
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

func (b *BinlogRestorer) getBinlogs(currentBinlogFile string) ([]string, error) {
	binlogs, err := fileutil.ListFiles(b.binlogDir, "", ".index")
	if err != nil {
		return nil, fmt.Errorf("fail to list binlog files from %s, error: %v", b.binlogDir, err)
	}

	sort.Slice(binlogs, func(i, j int) bool {
		ni := extractBinlogNumber(binlogs[i])
		nj := extractBinlogNumber(binlogs[j])
		return ni < nj
	})

	startIndex := -1

	fmt.Println(binlogs)
	currentBinlogFile = "/Users/julian.li/Downloads/binlog-restore/mbcpq7yjencg5rjkzuvg0h09/u04fin9nfjtunc497zb1azh9/binlogs/mysql-bin.000002"

	for i, name := range binlogs {
		if name == currentBinlogFile {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		return nil, fmt.Errorf("current binlog %s not found", currentBinlogFile)
	}

	return binlogs[startIndex:], nil
}

func (b *BinlogRestorer) createBinlogRestorePlan() (*binlogRestorePlan, error) {
	binlogInfo, err := b.binlogInfoQuerier.GetBinlogInfo()
	if err != nil {
		return nil, fmt.Errorf("fail to get binlog info, error: %v", err)
	}

	binlogs, err := b.getBinlogs(binlogInfo.currentBinlogFile)
	if err != nil {
		return nil, err
	}

	parser := replication.NewBinlogParser()

	plan := &binlogRestorePlan{
		startPosition: binlogInfo.position,
		firstBinlog:   binlogInfo.currentBinlogFile,
		binlogs:       make([]string, 0),
	}

	for _, binlog := range binlogs {
		err := parser.ParseFile(binlog, 0, func(e *replication.BinlogEvent) error {
			lastEventTime := time.Unix(int64(e.Header.Timestamp), 0)
			pos := e.Header.LogPos

			fmt.Println("binlog file", binlog, "stop time", b.stopDateTime.Local(), "last event time:", lastEventTime, "position", pos)

			// The --stop-datetime option is exclusive in mysqlbinlog command
			// So lets keep the behavior consistent
			if !lastEventTime.Before(b.stopDateTime) {
				return ErrStopPositionFound
			}

			plan.binlogs = append(plan.binlogs, binlog)
			plan.stopPosition = int(e.Header.LogPos)
			return nil
		})

		if err != nil {
			if errors.Is(err, ErrStopPositionFound) {
				return plan, nil
			}

			return nil, fmt.Errorf("fail to parse binlog file: %s: %v", binlog, err)
		}
	}

	return plan, nil
}

// Use mysqlbinlog to restore data -> if --dry-run just output the content, otherwise pipe it with mysql
func (b *BinlogRestorer) Restore() error {
	plan, err := b.createBinlogRestorePlan()
	if err != nil {
		return fmt.Errorf("fail to extract stop position, error: %v", err)
	}

	fmt.Println(plan)

	// @TODO
	// if dry run output all event to stdout
	// otherwise chunk commands and run them one by one
	return nil
}

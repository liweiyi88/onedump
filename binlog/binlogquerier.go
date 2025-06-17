package binlog

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
)

const (
	ShowLogBinQuery         = "SHOW VARIABLES LIKE 'log_bin';"
	ShowMasterStatusQuery   = "SHOW MASTER STATUS;"
	ShowBinlogStatusQuery   = "SHOW BINARY LOG STATUS;"
	ShowLogBinBasenameQuery = "SHOW VARIABLES LIKE 'log_bin_basename';"
	VersionQuery            = "SELECT VERSION() AS mysql_version;"
)

type BinlogInfo struct {
	currentBinlogFile string // e.g. binlog.000001
	position          uint64
	binlogDir         string // the binlog folder
	binlogPrefix      string // the binlog prefix. e.g. binlog
}

type binlogQuerier struct {
	db *sql.DB
}

func NewBinlogQuerier(db *sql.DB) *binlogQuerier {
	return &binlogQuerier{
		db,
	}
}

func (b *binlogQuerier) queryVersion() (*mysqlVersion, error) {
	rows, err := b.db.Query(VersionQuery)

	if err != nil {
		return nil, fmt.Errorf("fail to run query %s, error: %v", VersionQuery, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", VersionQuery))
		}
	}()

	var version string
	if rows.Next() {
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("fail to scan database rows, query: %s, error: %v", VersionQuery, err)
		}
	}

	return splitServerVersion(version), nil
}

func (b *binlogQuerier) queryLogBin() error {
	rows, err := b.db.Query(ShowLogBinQuery)

	if err != nil {
		return fmt.Errorf("fail to run query %s, error: %v", ShowLogBinQuery, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", ShowLogBinQuery))
		}
	}()

	var variableName, logBin string
	if rows.Next() {
		if err := rows.Scan(&variableName, &logBin); err != nil {
			return fmt.Errorf("fail to scan database rows, query: %s, error: %v", ShowLogBinQuery, err)
		}
	}

	if logBin != "ON" {
		return fmt.Errorf("log_bin variable is %s; sync requires log_bin to be set to ON", logBin)
	}

	return nil
}

func (b *binlogQuerier) queryLogBinBasename() (string, error) {
	rows, err := b.db.Query(ShowLogBinBasenameQuery)
	if err != nil {
		return "", fmt.Errorf("fail to run query %s, error: %v", ShowLogBinBasenameQuery, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", ShowLogBinBasenameQuery))
		}
	}()

	var variableName, value string

	if rows.Next() {
		if err := rows.Scan(&variableName, &value); err != nil {
			return "", fmt.Errorf("fail to scan database rows, query: %s, error: %v", ShowLogBinBasenameQuery, err)
		}

		return value, nil
	}

	return "", errors.New("fail to get log bin basename result")
}

func (b *binlogQuerier) queryBinlogStatus() (string, uint64, error) {
	var currentBinlogFile string
	var position uint64
	var binlogDoDB, binlogIgnoreDB, executedGtidSet string

	version, err := b.queryVersion()
	if err != nil {
		return "", 0, fmt.Errorf("fail to query MySQL version, error: %v", err)
	}

	var showBinlogStatusQuery string
	// use ShowMasterStatusQuery for all MySQL < 8.2
	if version.major < 8 || (version.major == 8 && version.minor < 2) {
		showBinlogStatusQuery = ShowMasterStatusQuery
	} else {
		showBinlogStatusQuery = ShowBinlogStatusQuery
	}

	rows, err := b.db.Query(showBinlogStatusQuery)
	if err != nil {
		return "", 0, fmt.Errorf("fail to run query %s, error: %v", showBinlogStatusQuery, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", showBinlogStatusQuery))
		}
	}()

	if rows.Next() {
		if err := rows.Scan(&currentBinlogFile, &position, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet); err != nil {
			return "", 0, fmt.Errorf("fail to scan database rows, query: %s, error: %v", showBinlogStatusQuery, err)
		}

		return currentBinlogFile, position, nil
	}

	return "", 0, errors.New("fail to get binlog status result")
}

func (b *binlogQuerier) GetBinlogInfo() (*BinlogInfo, error) {
	if err := b.queryLogBin(); err != nil {
		return nil, err
	}

	currentBinlogFile, position, err := b.queryBinlogStatus()
	if err != nil {
		return nil, err
	}

	basename, err := b.queryLogBinBasename()
	if err != nil {
		return nil, err
	}

	binlogDir := filepath.Dir(basename)
	binlogPrefix := filepath.Base(basename)

	return &BinlogInfo{
		currentBinlogFile,
		position,
		binlogDir,
		binlogPrefix,
	}, nil
}

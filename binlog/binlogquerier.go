package binlog

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
)

const (
	SHOW_LOG_BIN_QUERY          = "SHOW VARIABLES LIKE 'log_bin';"
	SHOW_MASTER_STATUS_QUERY    = "SHOW MASTER STATUS;"
	SHOW_BINLOG_STATUS_QUERY    = "SHOW BINARY LOG STATUS;"
	SHOW_LOG_BIN_BASENAME_QUERY = "SHOW VARIABLES LIKE 'log_bin_basename';"
	VERSION_QUERY               = "SELECT VERSION() AS mysql_version;"
)

type BinlogInfo struct {
	currentBinlogFile string // e.g. binlog.000001
	binlogDir         string // the binlog folder
	binlogPrefix      string // the binlog prefix. e.g. binlog
}

type binlogInfoQuerier struct {
	db *sql.DB
}

func NewBinlogInfoQuerier(db *sql.DB) *binlogInfoQuerier {
	return &binlogInfoQuerier{
		db,
	}
}

func (b *binlogInfoQuerier) queryVersion() (*mysqlVersion, error) {
	rows, err := b.db.Query(VERSION_QUERY)

	if err != nil {
		return nil, fmt.Errorf("fail to run query %s, error: %v", VERSION_QUERY, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", VERSION_QUERY))
		}
	}()

	var version string
	if rows.Next() {
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("fail to scan database rows, query: %s, error: %v", VERSION_QUERY, err)
		}
	}

	return splitServerVersion(version), nil
}

func (b *binlogInfoQuerier) queryLogBin() error {
	rows, err := b.db.Query(SHOW_LOG_BIN_QUERY)

	if err != nil {
		return fmt.Errorf("fail to run query %s, error: %v", SHOW_LOG_BIN_QUERY, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", SHOW_LOG_BIN_QUERY))
		}
	}()

	var variableName, logBin string
	if rows.Next() {
		if err := rows.Scan(&variableName, &logBin); err != nil {
			return fmt.Errorf("fail to scan database rows, query: %s, error: %v", SHOW_LOG_BIN_QUERY, err)
		}
	}

	if logBin != "ON" {
		return fmt.Errorf("log_bin variable is %s; sync requires log_bin to be set to ON", logBin)
	}

	return nil
}

func (b *binlogInfoQuerier) queryLogBinBasename() (string, error) {
	rows, err := b.db.Query(SHOW_LOG_BIN_BASENAME_QUERY)
	if err != nil {
		return "", fmt.Errorf("fail to run query %s, error: %v", SHOW_LOG_BIN_BASENAME_QUERY, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", SHOW_LOG_BIN_BASENAME_QUERY))
		}
	}()

	var variableName, value string

	if rows.Next() {
		if err := rows.Scan(&variableName, &value); err != nil {
			return "", fmt.Errorf("fail to scan database rows, query: %s, error: %v", SHOW_LOG_BIN_BASENAME_QUERY, err)
		}

		return value, nil
	}

	return "", errors.New("fail to get log bin basename result")
}

func (b *binlogInfoQuerier) queryBinlogStatus() (string, error) {
	var currentBinlogFile string
	var position int
	var binlogDoDB, binlogIgnoreDB, executedGtidSet string

	version, err := b.queryVersion()
	if err != nil {
		return "", fmt.Errorf("fail to query MySQL version, error: %v", err)
	}

	showBinlogStatusQuery := SHOW_BINLOG_STATUS_QUERY

	if version.major <= 8 && version.minor < 2 {
		showBinlogStatusQuery = SHOW_MASTER_STATUS_QUERY
	}

	rows, err := b.db.Query(showBinlogStatusQuery)
	if err != nil {
		return "", fmt.Errorf("fail to run query %s, error: %v", showBinlogStatusQuery, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", showBinlogStatusQuery))
		}
	}()

	if rows.Next() {
		if err := rows.Scan(&currentBinlogFile, &position, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet); err != nil {
			return "", fmt.Errorf("fail to scan database rows, query: %s, error: %v", showBinlogStatusQuery, err)
		}

		return currentBinlogFile, nil
	}

	return "", errors.New("fail to get binlog status result")
}

func (b *binlogInfoQuerier) GetBinlogInfo() (*BinlogInfo, error) {
	if err := b.queryLogBin(); err != nil {
		return nil, err
	}

	currentBinlogFile, err := b.queryBinlogStatus()
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
		binlogDir,
		binlogPrefix,
	}, nil
}

package binlog

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
)

const (
	SHOW_LOG_BIN_QUERY       = "SHOW VARIABLES LIKE 'log_bin';"
	SHOW_MASTER_STATUS_QUERY = "SHOW MASTER STATUS;"
	SHOW_LOG_BIN_BASENAME    = "SHOW VARIABLES LIKE 'log_bin_basename';"
)

type BinlogInfo struct {
	currentBinlogFile string // currentBinlogFile string // e.g. binlog.000001
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
	for rows.Next() {
		if err := rows.Scan(&variableName, &logBin); err != nil {
			return fmt.Errorf("fail to scan database rows, query: %s, error: %v", SHOW_LOG_BIN_QUERY, err)
		}

		break
	}

	if logBin != "ON" {
		return fmt.Errorf("log_bin variable is %s; sync requires log_bin to be set to ON", logBin)
	}

	return nil
}

func (b *binlogInfoQuerier) queryLogBinBasename() (string, error) {
	rows, err := b.db.Query(SHOW_LOG_BIN_BASENAME)
	if err != nil {
		return "", fmt.Errorf("failt to run query %s, error: %v", SHOW_LOG_BIN_BASENAME, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", SHOW_LOG_BIN_BASENAME))
		}
	}()

	var variableName, value string

	for rows.Next() {
		if err := rows.Scan(&variableName, &value); err != nil {
			return "", fmt.Errorf("fail to scan database rows, query: %s, error: %v", SHOW_LOG_BIN_QUERY, err)
		}

		return value, nil
	}

	return "", errors.New("fail to get log bin basename result")
}

func (b *binlogInfoQuerier) queryMasterStatus() (string, error) {
	var currentBinlogFile string
	var position int
	var binlogDoDB, binlogIgnoreDB, excutedGtidSet string

	rows, err := b.db.Query(SHOW_MASTER_STATUS_QUERY)
	if err != nil {
		return "", fmt.Errorf("fail to run query %s, error: %v", SHOW_MASTER_STATUS_QUERY, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("fail to close database rows", slog.Any("error", err), slog.Any("query", SHOW_MASTER_STATUS_QUERY))
		}
	}()

	for rows.Next() {
		if err := rows.Scan(&currentBinlogFile, &position, &binlogDoDB, &binlogIgnoreDB, &excutedGtidSet); err != nil {
			return "", fmt.Errorf("fail to scan database rows, query: %s, error: %v", SHOW_MASTER_STATUS_QUERY, err)
		}

		return currentBinlogFile, nil
	}

	return "", errors.New("fail to get master status result")
}

func (b *binlogInfoQuerier) GetBinlogInfo() (*BinlogInfo, error) {
	if err := b.queryLogBin(); err != nil {
		return nil, err
	}

	currentBinlogFile, err := b.queryMasterStatus()
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

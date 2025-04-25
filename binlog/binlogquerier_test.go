package binlog

import (
	"errors"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestNewBinlogInfoQuerier(t *testing.T) {
	assert := assert.New(t)

	db, _, err := sqlmock.New()
	assert.NoError(err)
	defer db.Close()

	querier := NewBinlogInfoQuerier(db)
	assert.NotNil(querier)
	assert.Equal(db, querier.db)
}

func TestQueryLogBinSuccess(t *testing.T) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New()
	assert.NoError(err)
	defer db.Close()

	querier := NewBinlogInfoQuerier(db)

	// Mock successful response with log_bin ON
	rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "ON")
	mock.ExpectQuery(ShowLogBinQuery).WillReturnRows(rows)

	err = querier.queryLogBin()
	assert.NoError(err)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestQueryLogBinFailure(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.NoError(err)
	defer db.Close()

	querier := NewBinlogInfoQuerier(db)

	// Test case 1: Query error
	mock.ExpectQuery(ShowLogBinQuery).WillReturnError(errors.New("query error"))
	err = querier.queryLogBin()
	assert.Error(err)
	assert.Contains(err.Error(), "fail to run query")
	assert.NoError(mock.ExpectationsWereMet())

	// Test case 2: Scan error
	rows := sqlmock.NewRows([]string{"Variable_name"}).AddRow("log_bin") // missing Value column
	mock.ExpectQuery(ShowLogBinQuery).WillReturnRows(rows)
	err = querier.queryLogBin()
	assert.Error(err)
	assert.Contains(err.Error(), "fail to scan database rows")
	assert.NoError(mock.ExpectationsWereMet())

	// Test case 3: log_bin not ON
	rows = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "OFF")
	mock.ExpectQuery(ShowLogBinQuery).WillReturnRows(rows)
	err = querier.queryLogBin()
	assert.Error(err)
	assert.Contains(err.Error(), "log_bin variable is OFF")
	assert.NoError(mock.ExpectationsWereMet())
}

func TestQueryLogBinBasenameSuccess(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.NoError(err)
	defer db.Close()

	querier := NewBinlogInfoQuerier(db)

	expectedValue := filepath.Join("var", "log", "mysql", "mysql-bin")
	rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin_basename", expectedValue)
	mock.ExpectQuery(ShowLogBinBasenameQuery).WillReturnRows(rows)

	value, err := querier.queryLogBinBasename()
	assert.NoError(err)
	assert.Equal(expectedValue, value)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestQueryLogBinBasenameFailure(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.NoError(err)
	defer db.Close()

	querier := NewBinlogInfoQuerier(db)

	mock.ExpectQuery(ShowLogBinBasenameQuery).WillReturnError(errors.New("query error"))
	value, err := querier.queryLogBinBasename()
	assert.Error(err)
	assert.Empty(value)
	assert.Contains(err.Error(), "fail to run query")
	assert.NoError(mock.ExpectationsWereMet())

	rows := sqlmock.NewRows([]string{"Variable_name"}).AddRow("log_bin_basename") // missing Value column
	mock.ExpectQuery(ShowLogBinBasenameQuery).WillReturnRows(rows)
	value, err = querier.queryLogBinBasename()
	assert.Error(err)
	assert.Empty(value)
	assert.Contains(err.Error(), "fail to scan database rows")
	assert.NoError(mock.ExpectationsWereMet())

	rows = sqlmock.NewRows([]string{"Variable_name", "Value"}) // empty
	mock.ExpectQuery(ShowLogBinBasenameQuery).WillReturnRows(rows)
	value, err = querier.queryLogBinBasename()
	assert.Error(err)
	assert.Empty(value)
	assert.Equal("fail to get log bin basename result", err.Error())
	assert.NoError(mock.ExpectationsWereMet())
}

func TestQueryBinlogStatusSuccess(t *testing.T) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New()
	assert.NoError(err)
	defer db.Close()

	querier := NewBinlogInfoQuerier(db)

	rows := sqlmock.NewRows([]string{"mysql_version"}).AddRow("8.0.42")
	mock.ExpectQuery(regexp.QuoteMeta(VersionQuery)).WillReturnRows(rows)

	expectedFile := "mysql-bin.000123"
	rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(expectedFile, 1234, "", "", "")
	mock.ExpectQuery(ShowMasterStatusQuery).WillReturnRows(rows)

	file, err := querier.queryBinlogStatus()
	assert.NoError(err)
	assert.Equal(expectedFile, file)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestQueryBinlogStatusFailure(t *testing.T) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New()
	assert.NoError(err)
	defer db.Close()

	querier := NewBinlogInfoQuerier(db)

	rows := sqlmock.NewRows([]string{"mysql_version"}).AddRow("8.2.42")
	mock.ExpectQuery(regexp.QuoteMeta(VersionQuery)).WillReturnRows(rows)

	// Test case 1: Query error
	mock.ExpectQuery(ShowBinlogStatusQuery).WillReturnError(errors.New("query error"))
	file, err := querier.queryBinlogStatus()
	assert.Error(err)
	assert.Empty(file)
	assert.Contains(err.Error(), "fail to run query")
	assert.NoError(mock.ExpectationsWereMet())

	// Test case 2: Scan error
	rows = sqlmock.NewRows([]string{"mysql_version"}).AddRow("8.2.42")
	mock.ExpectQuery(regexp.QuoteMeta(VersionQuery)).WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"File"}).AddRow("mysql-bin.000123") // missing other columns
	mock.ExpectQuery(ShowBinlogStatusQuery).WillReturnRows(rows)
	file, err = querier.queryBinlogStatus()
	assert.Error(err)
	assert.Empty(file)
	assert.Contains(err.Error(), "fail to scan database rows")
	assert.NoError(mock.ExpectationsWereMet())

	// Test case 3: No rows returned
	rows = sqlmock.NewRows([]string{"mysql_version"}).AddRow("8.2.42")
	mock.ExpectQuery(regexp.QuoteMeta(VersionQuery)).WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}) // empty
	mock.ExpectQuery(ShowBinlogStatusQuery).WillReturnRows(rows)
	file, err = querier.queryBinlogStatus()
	assert.Error(err)
	assert.Empty(file)
	assert.Equal("fail to get binlog status result", err.Error())
	assert.NoError(mock.ExpectationsWereMet())
}

func TestRowsCloseError(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.NoError(err)
	defer db.Close()

	querier := NewBinlogInfoQuerier(db)

	// Test rows.Close() error for queryLogBin
	rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "ON").CloseError(errors.New("close error"))
	mock.ExpectQuery(ShowLogBinQuery).WillReturnRows(rows)
	err = querier.queryLogBin()
	assert.NoError(err) // The close error should be logged but not returned
	assert.NoError(mock.ExpectationsWereMet())

	// Test rows.Close() error for queryLogBinBasename
	rows = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin_basename", "/path").CloseError(errors.New("close error"))
	mock.ExpectQuery(ShowLogBinBasenameQuery).WillReturnRows(rows)
	_, err = querier.queryLogBinBasename()
	assert.NoError(err) // The close error should be logged but not returned
	assert.NoError(mock.ExpectationsWereMet())

	// Test rows.Close() error for queryBinlogStatus
	rows = sqlmock.NewRows([]string{"mysql_version"}).AddRow("8.2.42")
	mock.ExpectQuery(regexp.QuoteMeta(VersionQuery)).WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow("mysql-bin.000123", 123, "", "", "").CloseError(errors.New("close error"))
	mock.ExpectQuery(ShowBinlogStatusQuery).WillReturnRows(rows)
	_, err = querier.queryBinlogStatus()
	assert.NoError(err) // The close error should be logged but not returned
	assert.NoError(mock.ExpectationsWereMet())
}

func TestGetBinlogInfo(t *testing.T) {
	assert := assert.New(t)

	t.Run("it should return error if it can not query log bin info", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		assert.NoError(err)

		querier := NewBinlogInfoQuerier(db)

		rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("log_bin", "OFF").CloseError(errors.New("close error"))
		mock.ExpectQuery(ShowLogBinQuery).WillReturnRows(rows)

		_, err = querier.GetBinlogInfo()
		assert.Error(err)
	})

	t.Run("it should return error if it can not query binlog status", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		assert.NoError(err)

		querier := NewBinlogInfoQuerier(db)

		rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("log_bin", "ON").CloseError(errors.New("close error"))
		mock.ExpectQuery(ShowLogBinQuery).WillReturnRows(rows)

		mock.ExpectQuery(ShowBinlogStatusQuery).WillReturnError(errors.New("query status error"))

		_, err = querier.GetBinlogInfo()
		assert.Error(err)
	})

	t.Run("it should return error if it can not query binlog basename", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		assert.NoError(err)

		querier := NewBinlogInfoQuerier(db)

		rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("log_bin", "ON").CloseError(errors.New("close error"))
		mock.ExpectQuery(ShowLogBinQuery).WillReturnRows(rows)

		expectedFile := "mysql-bin.000123"
		rows = sqlmock.NewRows([]string{"mysql_version"}).AddRow("8.2.42")
		mock.ExpectQuery(regexp.QuoteMeta(VersionQuery)).WillReturnRows(rows)

		rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
			AddRow(expectedFile, 1234, "", "", "")
		mock.ExpectQuery(ShowBinlogStatusQuery).WillReturnRows(rows)

		mock.ExpectQuery(ShowLogBinBasenameQuery).WillReturnError(errors.New("query log bin basename error"))
		_, err = querier.GetBinlogInfo()

		assert.Error(err)
	})

	t.Run("it should get binlog info struct", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		assert.NoError(err)

		querier := NewBinlogInfoQuerier(db)

		rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("log_bin", "ON").CloseError(errors.New("close error"))
		mock.ExpectQuery(ShowLogBinQuery).WillReturnRows(rows)

		expectedFile := "mysql-bin.000123"

		rows = sqlmock.NewRows([]string{"mysql_version"}).AddRow("8.2.42")
		mock.ExpectQuery(regexp.QuoteMeta(VersionQuery)).WillReturnRows(rows)

		rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
			AddRow(expectedFile, 1234, "", "", "")
		mock.ExpectQuery(ShowBinlogStatusQuery).WillReturnRows(rows)

		expectedValue := filepath.Join("var", "log", "mysql", "mysql-bin")
		rows = sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("log_bin_basename", expectedValue)
		mock.ExpectQuery(ShowLogBinBasenameQuery).WillReturnRows(rows)

		info, err := querier.GetBinlogInfo()
		assert.NoError(err)

		expectedDir := filepath.Join("var", "log", "mysql")
		assert.Equal(expectedDir, info.binlogDir)
		assert.Equal("mysql-bin", info.binlogPrefix)
		assert.Equal("mysql-bin.000123", info.currentBinlogFile)
	})
	t.Run("version < 8.2 uses SHOW MASTER STATUS", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		assert.NoError(err)
		defer db.Close()

		querier := NewBinlogInfoQuerier(db)

		rows := sqlmock.NewRows([]string{"mysql_version"}).AddRow("5.7.1")
		mock.ExpectQuery(regexp.QuoteMeta(VersionQuery)).WillReturnRows(rows)

		expectedFile := "mysql-bin.000123"
		rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
			AddRow(expectedFile, 1234, "", "", "")
		mock.ExpectQuery(ShowMasterStatusQuery).WillReturnRows(rows)

		file, err := querier.queryBinlogStatus()
		assert.NoError(err)
		assert.Equal(expectedFile, file)
		assert.NoError(mock.ExpectationsWereMet())
	})

	t.Run("version >= 8.2 uses SHOW BINARY LOG STATUS", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		assert.NoError(err)
		defer db.Close()

		querier := NewBinlogInfoQuerier(db)

		rows := sqlmock.NewRows([]string{"mysql_version"}).AddRow("8.2.0")
		mock.ExpectQuery(regexp.QuoteMeta(VersionQuery)).WillReturnRows(rows)

		expectedFile := "mysql-bin.000456"
		rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
			AddRow(expectedFile, 1234, "", "", "")
		mock.ExpectQuery(ShowBinlogStatusQuery).WillReturnRows(rows)

		file, err := querier.queryBinlogStatus()
		assert.NoError(err)
		assert.Equal(expectedFile, file)
		assert.NoError(mock.ExpectationsWereMet())
	})
}

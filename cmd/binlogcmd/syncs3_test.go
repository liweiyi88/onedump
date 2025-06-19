package binlogcmd_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/liweiyi88/onedump/binlog"
	"github.com/liweiyi88/onedump/cmd"
	"github.com/liweiyi88/onedump/cmd/binlogcmd"
	"github.com/liweiyi88/onedump/env"
	"github.com/stretchr/testify/assert"
)

func TestSyncS3Cmd(t *testing.T) {
	assert := assert.New(t)
	cmd := cmd.RootCmd
	t.Setenv("AWS_ACCESS_KEY_ID", "access_key")
	t.Setenv("AWS_REGION", "ap-southeast-2")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-key")
	t.Setenv("DATABASE_DSN", "root:root@tcp(127.0.0.1:33044)/")

	mockDB, mock, err := sqlmock.New()
	assert.NoError(err)

	binlogcmd.OpenDB = func(dsn string) (*sql.DB, error) {
		return mockDB, nil
	}

	defer func() {
		binlogcmd.OpenDB = func(dsn string) (*sql.DB, error) {
			return sql.Open("mysql", dsn)
		}
	}()

	rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "ON")
	mock.ExpectQuery(binlog.ShowLogBinQuery).WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"mysql_version"}).AddRow("8.0.42")
	mock.ExpectQuery(regexp.QuoteMeta(binlog.VersionQuery)).WillReturnRows(rows)

	expectedFile := "mysql-bin.000123"
	rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(expectedFile, 1234, "", "", "")
	mock.ExpectQuery(binlog.ShowMasterStatusQuery).WillReturnRows(rows)

	expectedValue := filepath.Join("var", "log", "mysql", "mysql-bin")
	rows = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin_basename", expectedValue)

	mock.ExpectQuery(binlog.ShowLogBinBasenameQuery).WillReturnRows(rows)

	cmd.SetArgs([]string{"binlog", "sync", "s3", "--checksum=true", "--s3-bucket", "onedump", "--s3-prefix", "prefix", "--save-log=true"})
	err = cmd.Execute()
	assert.Error(err)
}

func TestValidateEnvVars(t *testing.T) {
	tests := []struct {
		name        string
		vars        []string
		setupEnv    func()
		cleanupEnv  func()
		expectError bool
	}{
		{
			name: "all variables present",
			vars: []string{"VAR1", "VAR2"},
			setupEnv: func() {
				os.Setenv("VAR1", "value1")
				os.Setenv("VAR2", "value2")
			},
			cleanupEnv: func() {
				os.Unsetenv("VAR1")
				os.Unsetenv("VAR2")
			},
			expectError: false,
		},
		{
			name: "one variable missing",
			vars: []string{"VAR1", "VAR2"},
			setupEnv: func() {
				os.Setenv("VAR1", "value1")
			},
			cleanupEnv: func() {
				os.Unsetenv("VAR1")
			},
			expectError: true,
		},
		{
			name:        "all variables missing",
			vars:        []string{"VAR1", "VAR2"},
			setupEnv:    func() {},
			cleanupEnv:  func() {},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupEnv()
			defer tt.cleanupEnv()

			err := env.EnsureRequiredVars(tt.vars)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

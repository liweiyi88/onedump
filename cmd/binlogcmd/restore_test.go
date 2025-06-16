package binlogcmd_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/liweiyi88/onedump/cmd"
	"github.com/liweiyi88/onedump/cmd/binlogcmd"
	"github.com/stretchr/testify/assert"
)

func TestRestoreMissingRequiredArgs(t *testing.T) {
	assert := assert.New(t)
	cmd := cmd.RootCmd

	cmd.SetArgs([]string{"binlog", "restore", "--dry-run=true", "--dir=/unknown"})
	err := cmd.Execute()

	assert.Error(err)
	assert.Equal("at least one of the flags in the group [start-binlog dump-file] is required", err.Error())
}

func TestMissingRequiredEnvs(t *testing.T) {
	assert := assert.New(t)
	cmd := cmd.RootCmd

	cmd.SetArgs([]string{"binlog", "restore", "--dry-run=true", "--dir=/unknown", "--dump-file=fake-dump.sql"})
	err := cmd.Execute()

	assert.Error(err)
	assert.Equal("missing required environment variable DATABASE_DSN", err.Error())
}

func TestRestoreWithDumpFileVariants(t *testing.T) {
	assert := assert.New(t)

	currentDir, err := os.Getwd()
	assert.NoError(err)

	binlogsDir := filepath.Join(currentDir, "..", "..", "testutils", "mysqlrestore", "binlogs")

	os.Setenv("DATABASE_DSN", "root:root@tcp(127.0.0.1:33044)/")

	t.Run("with plain dump file", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		binlogcmd.OpenDB = func(dsn string) (*sql.DB, error) {
			return mockDB, nil
		}

		defer func() {
			binlogcmd.OpenDB = func(dsn string) (*sql.DB, error) {
				return sql.Open("mysql", dsn)
			}
		}()

		dumpFilePath := filepath.Join(currentDir, "..", "..", "testutils", "mysqlrestore", "init-db.sql")

		cmd := cmd.RootCmd
		cmd.SetArgs([]string{"binlog", "restore", "--dry-run=true", "--dir=" + binlogsDir, "--dump-file=" + dumpFilePath})
		err = cmd.Execute()
		assert.NoError(err)
	})

	t.Run("with gzipped dump file", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		binlogcmd.OpenDB = func(dsn string) (*sql.DB, error) {
			return mockDB, nil
		}

		defer func() {
			binlogcmd.OpenDB = func(dsn string) (*sql.DB, error) {
				return sql.Open("mysql", dsn)
			}
		}()
		dumpFilePath := filepath.Join(currentDir, "..", "..", "testutils", "mysqlrestore", "init-db.sql.gz")
		cmd := cmd.RootCmd
		cmd.SetArgs([]string{"binlog", "restore", "--dry-run=true", "--dir=" + binlogsDir, "--dump-file=" + dumpFilePath})
		err = cmd.Execute()
		assert.NoError(err)
	})
}

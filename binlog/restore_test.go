package binlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseBinlogFilePosition(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantFile string
		wantPos  int
		wantErr  bool
	}{
		{
			name:     "MySQL > 8.0.23",
			input:    "CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='mysql-bin.000003', SOURCE_LOG_POS=1638;",
			wantFile: "mysql-bin.000003",
			wantPos:  1638,
		},
		{
			name:     "MySQL <= 8.0.23",
			input:    "CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000003', MASTER_LOG_POS=1638;",
			wantFile: "mysql-bin.000003",
			wantPos:  1638,
		},
	}

	assert := assert.New(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, pos, err := ParseBinlogFilePosition(strings.NewReader(tt.input))
			assert.NoError(err)
			assert.Equal(tt.wantFile, file)
			assert.Equal(tt.wantPos, pos)
		})
	}

	t.Run("return err if file and position are not found", func(t *testing.T) {
		reader := strings.NewReader("invalid content")
		file, pos, err := ParseBinlogFilePosition(reader)
		assert.Error(err)
		assert.Equal("", file)
		assert.Equal(0, pos)
	})
}

func TestGetSortedBinlogs(t *testing.T) {

	assert := assert.New(t)

	t.Run("it should return ErrBinlogsNotFound if no binlogs are found in the dir", func(t *testing.T) {
		tempDir, err := os.MkdirTemp(os.TempDir(), "binlog-test")
		assert.NoError(err)

		defer func() {
			err := os.RemoveAll(tempDir)
			assert.NoError(err)
		}()

		restorer := NewBinlogRestorer(tempDir, "mysqlbin.000005", 123)
		_, err = restorer.getSortedBinlogs()
		assert.ErrorIs(err, ErrBinlogsNotFound)
	})

	t.Run("it should current binlog not found err", func(t *testing.T) {
		tempDir, err := os.MkdirTemp(os.TempDir(), "binlog-test")
		assert.NoError(err)

		defer func() {
			err := os.RemoveAll(tempDir)
			assert.NoError(err)
		}()

		tempFile, err := os.Create(filepath.Join(tempDir, "mysqlbin.000001"))
		assert.NoError(err)

		defer func() {
			err := tempFile.Close()
			assert.NoError(err)
		}()

		restorer := NewBinlogRestorer(tempDir, "mysqlbin.000005", 123)
		_, err = restorer.getSortedBinlogs()
		assert.Error(err)
		assert.Equal(err.Error(), "current binlog mysqlbin.000005 not found")
	})

	t.Run("it should return sorted binlogs starting from the specified start binlog", func(t *testing.T) {
		tempDir, err := os.MkdirTemp(os.TempDir(), "binlog-test")
		assert.NoError(err)

		defer func() {
			err := os.RemoveAll(tempDir)
			assert.NoError(err)
		}()

		for i := range 10 {
			filename := fmt.Sprintf("mysqlbin.00000%d", i)
			tempFile, err := os.Create(filepath.Join(tempDir, filename))
			assert.NoError(err)

			defer func() {
				err := tempFile.Close()
				assert.NoError(err)
			}()
		}

		restorer := NewBinlogRestorer(tempDir, "mysqlbin.000005", 123)
		binlogs, err := restorer.getSortedBinlogs()
		assert.NoError(err)
		assert.Len(binlogs, 5)
		for i := 5; i < 10; i++ {
			assert.Equal(binlogs[i-5], filepath.Join(tempDir, fmt.Sprintf("mysqlbin.00000%d", i)))
		}
	})
}

func TestGetRestoreCommands(t *testing.T) {
	assert := assert.New(t)
	restorer := NewBinlogRestorer("", "mysqlbin.00001", 123)

	t.Run("it should return empty commands if plan's binlogs are empty", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		commands := restorer.getRestoreCommandArgs(plan)
		assert.Len(commands, 0)
	})

	t.Run("it should return empty commands if plan has only one binlog and start and stop position are the same", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{"mysqlbin.00001"}
		stopPos := 123
		plan.stopPosition = &stopPos
		commands := restorer.getRestoreCommandArgs(plan)
		assert.Len(commands, 0)
	})

	t.Run("it should return one command if plan has only one binlog when stop position is not specified", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{"mysqlbin.00001"}
		commands := restorer.getRestoreCommandArgs(plan)
		assert.Len(commands, 1)
		assert.Equal("mysqlbin.00001 --start-position=123", commands[0])
	})

	t.Run("it should return one command if plan has only one binlog when both start and stop position are specified", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{"mysqlbin.00001"}
		stopPos := 140
		plan.stopPosition = &stopPos
		commands := restorer.getRestoreCommandArgs(plan)
		assert.Len(commands, 1)
		assert.Equal("mysqlbin.00001 --start-position=123 --stop-position=140", commands[0])
	})

	t.Run("it should return multiple commands that have no chunked parts when stop position is not set", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{"mysqlbin.00001", "mysqlbin.00002", "mysqlbin.00003", "mysqlbin.00004"}
		commands := restorer.getRestoreCommandArgs(plan)
		assert.Len(commands, 3)
		assert.Equal("mysqlbin.00001 --start-position=123", commands[0])
		assert.Equal("mysqlbin.00002 mysqlbin.00003", commands[1])
		assert.Equal("mysqlbin.00004", commands[2])
	})

	t.Run("it should return multiple commands that have no chunked parts when stop position is set", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{"mysqlbin.00001", "mysqlbin.00002", "mysqlbin.00003", "mysqlbin.00004"}
		stopPos := 140
		plan.stopPosition = &stopPos
		commands := restorer.getRestoreCommandArgs(plan)
		assert.Len(commands, 3)
		assert.Equal("mysqlbin.00001 --start-position=123", commands[0])
		assert.Equal("mysqlbin.00002 mysqlbin.00003", commands[1])
		assert.Equal("mysqlbin.00004 --stop-position=140", commands[2])
	})

	t.Run("it should return multiple commands that have chunked parts", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{
			"mysqlbin.000001",
			"mysqlbin.000002",
			"mysqlbin.000003",
			"mysqlbin.000004",
			"mysqlbin.000005",
			"mysqlbin.000006",
			"mysqlbin.000007",
			"mysqlbin.000008",
			"mysqlbin.000009",
			"mysqlbin.000010",
			"mysqlbin.000011",
			"mysqlbin.000012",
			"mysqlbin.000013",
		}
		stopPos := 140
		plan.stopPosition = &stopPos
		commands := restorer.getRestoreCommandArgs(plan)
		assert.Len(commands, 4)
		assert.Equal("mysqlbin.000001 --start-position=123", commands[0])
		assert.Equal("mysqlbin.000002 mysqlbin.000003 mysqlbin.000004 mysqlbin.000005 mysqlbin.000006 mysqlbin.000007 mysqlbin.000008 mysqlbin.000009 mysqlbin.000010 mysqlbin.000011", commands[1])
		assert.Equal("mysqlbin.000012", commands[2])
		assert.Equal("mysqlbin.000013 --stop-position=140", commands[3])
	})
}

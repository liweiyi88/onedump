package binlog

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getTestMySQLCliPaths() (string, string) {
	currentDir, err := os.Getwd()
	if err != nil {
		panic(fmt.Sprintf("Could not get current work dir: %v", err))
	}

	basePath := filepath.Join(currentDir, "..", "testutils", "mysqlrestore")

	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(basePath, "mysqlbinlog_darwin"), filepath.Join(basePath, "mysql_darwin")
	case "linux":
		return filepath.Join(basePath, "mysqlbinlog_linux"), filepath.Join(basePath, "mysql_linux")
	case "windows":
		return filepath.Join(basePath, "mysqlbinlog.exe"), filepath.Join(basePath, "mysql.exe")
	default:
		panic(fmt.Sprintf("Unsupported OS: %s for test", runtime.GOOS))
	}
}

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
		assert.Equal("current binlog mysqlbin.000005 not found", err.Error())
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
		commands := restorer.createRestoreCommandArgs(plan)
		assert.Len(commands, 0)
	})

	t.Run("it should return empty commands if plan has only one binlog and start and stop position are the same", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{"mysqlbin.00001"}
		stopPos := 123
		plan.stopPosition = &stopPos
		commands := restorer.createRestoreCommandArgs(plan)
		assert.Len(commands, 0)
	})

	t.Run("it should return one command if plan has only one binlog when stop position is not specified", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{"mysqlbin.00001"}
		commands := restorer.createRestoreCommandArgs(plan)
		assert.Len(commands, 1)
		assert.Equal("mysqlbin.00001 --start-position=123", commands[0])
	})

	t.Run("it should return one command if plan has only one binlog when both start and stop position are specified", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{"mysqlbin.00001"}
		stopPos := 140
		plan.stopPosition = &stopPos
		commands := restorer.createRestoreCommandArgs(plan)
		assert.Len(commands, 1)
		assert.Equal("mysqlbin.00001 --start-position=123 --stop-position=140", commands[0])
	})

	t.Run("it should return multiple commands that have no chunked parts when stop position is not set", func(t *testing.T) {
		plan := newBinlogRestorePlan(123)
		plan.binlogs = []string{"mysqlbin.00001", "mysqlbin.00002", "mysqlbin.00003", "mysqlbin.00004"}
		commands := restorer.createRestoreCommandArgs(plan)
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
		commands := restorer.createRestoreCommandArgs(plan)
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
		commands := restorer.createRestoreCommandArgs(plan)
		assert.Len(commands, 4)
		assert.Equal("mysqlbin.000001 --start-position=123", commands[0])
		assert.Equal("mysqlbin.000002 mysqlbin.000003 mysqlbin.000004 mysqlbin.000005 mysqlbin.000006 mysqlbin.000007 mysqlbin.000008 mysqlbin.000009 mysqlbin.000010 mysqlbin.000011", commands[1])
		assert.Equal("mysqlbin.000012", commands[2])
		assert.Equal("mysqlbin.000013 --stop-position=140", commands[3])
	})
}

func TestWithOptions(t *testing.T) {
	restorer := NewBinlogRestorer(
		"",
		"",
		0,
		WithDatabaseDSN("root:root@tcp(127.0.0.1:33044)/"),
		WithDryRun(true),
		WithMySQLPath("mysql"),
		WithMySQLBinlogPath("mysqlbinlog"),
		WithStopDateTime("2025-03-04 12:10:10"),
	)

	stopDateTime, err := time.Parse(time.DateTime, "2025-03-04 12:10:10")
	assert.NoError(t, err)

	expected := &BinlogRestorer{
		binlogDir:       "",
		dryRun:          true,
		dsn:             "root:root@tcp(127.0.0.1:33044)/",
		mysqlPath:       "mysql",
		mysqlbinlogPath: "mysqlbinlog",
		startBinlog:     "",
		startPosition:   0,
		stopDateTime:    stopDateTime,
	}

	assert.Equal(t, expected, restorer)
}

func TestCreateBinlogRestorePlan(t *testing.T) {
	assert := assert.New(t)

	currentDir, err := os.Getwd()
	assert.NoError(err)

	binlogsDir := filepath.Join(currentDir, "..", "testutils", "mysqlrestore", "binlogs")

	t.Run("it should create the restore plan if no value of --stop-datetime optio is passed", func(t *testing.T) {
		restorer := NewBinlogRestorer(
			binlogsDir,
			"mysql-bin.000002",
			0,
			WithDatabaseDSN("root:root@tcp(127.0.0.1:33044)/"),
			WithDryRun(true),
			WithMySQLPath("mysql"),
			WithMySQLBinlogPath("mysqlbinlog"),
		)

		plan, err := restorer.createBinlogRestorePlan()
		assert.NoError(err)

		expected := &binlogRestorePlan{
			startPosition: 0,
			stopPosition:  nil,
			binlogs:       []string{filepath.Join(binlogsDir, "mysql-bin.000002"), filepath.Join(binlogsDir, "mysql-bin.000003")},
		}

		assert.Equal(expected, plan)
	})

	t.Run("it should return test stop position not found error", func(t *testing.T) {
		restorer := NewBinlogRestorer(
			binlogsDir,
			"mysql-bin.000001",
			0,
			WithDatabaseDSN("root:root@tcp(127.0.0.1:33044)/"),
			WithDryRun(true),
			WithMySQLPath("mysql"),
			WithMySQLBinlogPath("mysqlbinlog"),
			WithStopDateTime("2025-03-04 12:10:10"),
		)

		_, err := restorer.createBinlogRestorePlan()
		assert.ErrorIs(err, ErrStopPositionNotFound)
	})

	t.Run("it should create the plan with stop position that is not the last one from the binlog files", func(t *testing.T) {
		restorer := NewBinlogRestorer(
			binlogsDir,
			"mysql-bin.000002",
			0,
			WithDatabaseDSN("root:root@tcp(127.0.0.1:33044)/"),
			WithDryRun(true),
			WithMySQLPath("mysql"),
			WithMySQLBinlogPath("mysqlbinlog"),
			WithStopDateTime("2025-06-03 01:00:43"),
		)

		plan, err := restorer.createBinlogRestorePlan()
		assert.NoError(err)

		stopPos := 1857
		expected := &binlogRestorePlan{
			startPosition: 0,
			stopPosition:  &stopPos,
			binlogs:       []string{filepath.Join(binlogsDir, "mysql-bin.000002"), filepath.Join(binlogsDir, "mysql-bin.000003")},
		}

		assert.Equal(expected, plan)
	})

	t.Run("it should create the plan with stop position that is the last one from the binlog files", func(t *testing.T) {
		restorer := NewBinlogRestorer(
			binlogsDir,
			"mysql-bin.000002",
			0,
			WithDatabaseDSN("root:root@tcp(127.0.0.1:33044)/"),
			WithDryRun(true),
			WithMySQLPath("mysql"),
			WithMySQLBinlogPath("mysqlbinlog"),
			WithStopDateTime("2025-06-05 01:00:43"),
		)

		plan, err := restorer.createBinlogRestorePlan()
		assert.NoError(err)

		stopPos := 2609
		expected := &binlogRestorePlan{
			startPosition: 0,
			stopPosition:  &stopPos,
			binlogs:       []string{filepath.Join(binlogsDir, "mysql-bin.000002"), filepath.Join(binlogsDir, "mysql-bin.000003")},
		}

		assert.Equal(expected, plan)
	})
}

func TestEnsureMySQLCommandPaths(t *testing.T) {
	assert := assert.New(t)
	restorer := NewBinlogRestorer("", "", 0, WithMySQLBinlogPath("notfound"))

	err := restorer.EnsureMySQLCommandPaths()
	assert.Error(err)

	mysqlbinlogPath, mysqlPath := getTestMySQLCliPaths()

	restorer = NewBinlogRestorer("", "", 0, WithMySQLBinlogPath(mysqlbinlogPath), WithMySQLPath("notfound"))
	err = restorer.EnsureMySQLCommandPaths()
	assert.Error(err)

	restorer = NewBinlogRestorer("", "", 0, WithMySQLBinlogPath(mysqlbinlogPath), WithMySQLPath(mysqlPath))
	err = restorer.EnsureMySQLCommandPaths()
	assert.NoError(err)
}

func TestRestore(t *testing.T) {
	assert := assert.New(t)

	currentDir, err := os.Getwd()
	assert.NoError(err)

	t.Log(runtime.GOOS)

	binlogsDir := filepath.Join(currentDir, "..", "testutils", "mysqlrestore", "binlogs")
	mysqlbinlogPath, mysqlPath := getTestMySQLCliPaths()

	t.Run("it should print results when passing --dry-run=true", func(t *testing.T) {
		restorer := NewBinlogRestorer(
			binlogsDir,
			"mysql-bin.000003",
			0,
			WithDatabaseDSN("root:root@tcp(127.0.0.1:33044)/"),
			WithDryRun(true),
			WithMySQLPath(mysqlPath),
			WithMySQLBinlogPath(mysqlbinlogPath),
			WithStopDateTime("2025-06-05 01:00:43"),
		)

		err := restorer.Restore()
		assert.NoError(err)
	})

	t.Run("it should restore", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			// The piping on Window is different from Linux, on CI server the Antivirus/Windows Defender will also slow down the program
			// They will just cause the test timeout. So lets skip this test on windows machine.
			t.Skip("Skipping this mysqlbinlog and mysql pipe on Windows due to known performance issues with external commands.")
		}

		restorer := NewBinlogRestorer(
			binlogsDir,
			"mysql-bin.000003",
			0,
			WithDatabaseDSN("root:root@tcp(127.0.0.1:33044)/"),
			WithMySQLPath(mysqlPath),
			WithMySQLBinlogPath(mysqlbinlogPath),
			WithStopDateTime("2025-06-05 01:00:43"),
		)

		err := restorer.Restore()
		assert.NoError(err)
	})
}

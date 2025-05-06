package binlog

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/require"
)

// MockStorage is a mock implementation of the Storage interface
type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) Save(reader io.Reader, pathGenerator storage.PathGeneratorFunc) error {
	args := m.Called(reader, pathGenerator)
	return args.Error(0)
}

func TestBinlogSyncerSyncFile(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		setupMock   func(*MockStorage)
		checksum    bool
		expectError bool
	}{
		{
			name:     "successful sync",
			filename: "testfile.bin",
			setupMock: func(ms *MockStorage) {
				ms.On("Save", mock.Anything, mock.Anything).Return(nil)
			},
			checksum:    true,
			expectError: false,
		},
		{
			name:     "storage save fails",
			filename: "testfile.bin",
			setupMock: func(ms *MockStorage) {
				ms.On("Save", mock.Anything, mock.Anything).Return(errors.New("save failed"))
			},
			checksum:    false,
			expectError: true,
		},
		{
			name:        "file open fails",
			filename:    "nonexistent.bin",
			setupMock:   func(ms *MockStorage) {},
			checksum:    false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test file if needed
			if tt.filename != "nonexistent.bin" {
				f, err := os.Create(tt.filename)
				require.NoError(t, err)
				_, err = f.WriteString("test data")
				require.NoError(t, err)
				f.Close()
				defer os.Remove(tt.filename)
			}

			mockStorage := new(MockStorage)
			tt.setupMock(mockStorage)

			syncer := &BinlogSyncer{
				destinationPath: "/test/destination",
				checksum:        tt.checksum,
			}

			err := syncer.syncFile(tt.filename, mockStorage)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestBinlogSyncerSync(t *testing.T) {
	tests := []struct {
		name        string
		saveLog     bool
		checksum    bool
		setupFiles  func() (string, error)
		setupMock   func(*MockStorage, []string)
		expectError bool
	}{
		{
			name:     "successful sync of multiple files",
			saveLog:  true,
			checksum: true,
			setupFiles: func() (string, error) {
				dir, err := os.MkdirTemp("", "binlogtest")
				if err != nil {
					return "", err
				}

				files := []string{"binlog.000001", "binlog.000002", "binlog.000003"}
				for _, f := range files {
					path := filepath.Join(dir, f)
					if err := os.WriteFile(path, []byte("test data"), 0644); err != nil {
						return dir, err
					}
				}
				return dir, nil
			},
			setupMock: func(ms *MockStorage, files []string) {
				for range files {
					ms.On("Save", mock.Anything, mock.AnythingOfType("storage.PathGeneratorFunc")).
						Return(nil)
				}
			},
			expectError: false,
		},
		{
			name:     "partial failure during sync",
			saveLog:  false,
			checksum: false,
			setupFiles: func() (string, error) {
				dir, err := os.MkdirTemp("", "binlogtest")
				if err != nil {
					return "", err
				}

				files := []string{"binlog.000001", "binlog.000002", "binlog.000003"}
				for _, f := range files {
					path := filepath.Join(dir, f)
					if err := os.WriteFile(path, []byte("test data"), 0644); err != nil {
						return dir, err
					}
				}
				return dir, nil
			},
			setupMock: func(ms *MockStorage, files []string) {
				// First call succeeds
				ms.On("Save", mock.Anything, mock.AnythingOfType("storage.PathGeneratorFunc")).
					Return(nil).
					Once()
				// Second call fails
				ms.On("Save", mock.Anything, mock.AnythingOfType("storage.PathGeneratorFunc")).
					Return(errors.New("save failed")).
					Once()
				// Third call succeeds
				ms.On("Save", mock.Anything, mock.AnythingOfType("storage.PathGeneratorFunc")).
					Return(nil).
					Once()
			},
			expectError: true,
		},
		{
			name:     "no files to sync",
			saveLog:  false,
			checksum: true,
			setupFiles: func() (string, error) {
				dir, err := os.MkdirTemp("", "binlogtest")
				if err != nil {
					return "", err
				}
				return dir, nil
			},
			setupMock:   func(ms *MockStorage, files []string) {},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test files
			dir, err := tt.setupFiles()
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			// List files to know how many mocks we need
			files, err := fileutil.ListFiles(dir, "binlog*")
			require.NoError(t, err)

			mockStorage := new(MockStorage)
			tt.setupMock(mockStorage, files)

			syncer := &BinlogSyncer{
				BinlogInfo: &BinlogInfo{
					binlogDir:    dir,
					binlogPrefix: "binlog",
				},
				destinationPath: "/test/destination",
				checksum:        tt.checksum,
				saveLog:         tt.saveLog,
			}

			err = syncer.Sync(mockStorage)

			assert := assert.New(t)

			if tt.expectError {
				assert.Error(err)
			} else {
				assert.NoError(err)
			}

			syncResultFile := filepath.Join(dir, SyncResultFile)

			if tt.saveLog {
				assert.FileExists(syncResultFile)

				defer func() {
					err := os.Remove(syncResultFile)
					assert.NoError(err)
				}()
			} else {
				assert.NoFileExists(syncResultFile)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestNewBinlogSyncer(t *testing.T) {
	syncer := NewBinlogSyncer("/onedump", false, false, &BinlogInfo{
		currentBinlogFile: "mysql-bin.000001",
		binlogDir:         "/var/log/mysql",
		binlogPrefix:      "mysql-bin",
	})

	assert.Equal(t, "/onedump", syncer.destinationPath)
	assert.Equal(t, "mysql-bin.000001", syncer.currentBinlogFile)
	assert.Equal(t, "/var/log/mysql", syncer.binlogDir)
	assert.False(t, syncer.checksum)
	assert.Equal(t, "mysql-bin", syncer.binlogPrefix)
}

func TestSaveSyncResult(t *testing.T) {
	assert := assert.New(t)
	files := []string{"binlog0001", "binlog0002"}

	err := newSyncResult(files, nil).save("")
	assert.NoError(err)

	defer func() {
		err := os.Remove(SyncResultFile)
		assert.NoError(err)
	}()

	assert.FileExists(SyncResultFile)
}

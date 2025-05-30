package sftp

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"
	"time"

	"github.com/k0kubun/go-ansi"
	"github.com/liweiyi88/onedump/dumper/dialer"
	"github.com/liweiyi88/onedump/storage"
	"github.com/schollz/progressbar/v3"

	sftpdialer "github.com/pkg/sftp"
)

const (
	BaseDelay = 5 * time.Second
)

var ErrNonRetryable = errors.New("non-retryable error")

type Result struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error"`
	Written int64  `json:"written"`
}

type SftpConifg struct {
	Host, User, Key string
	MaxAttempts     int
}

type Sftp struct {
	mu          sync.Mutex
	written     int64 // number of bytes that have been written to the remote file
	attempts    int
	MaxAttempts int    // by default it is 0, infinite retries
	Path        string `yaml:"path"`
	SshHost     string `yaml:"sshhost"`
	SshUser     string `yaml:"sshuser"`
	SshKey      string `yaml:"sshkey"`
}

func NewSftp(config *SftpConifg) *Sftp {
	return &Sftp{
		SshHost:     config.Host,
		SshUser:     config.User,
		SshKey:      config.Key,
		MaxAttempts: config.MaxAttempts,
	}
}

func (sf *Sftp) reset() {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	sf.attempts = 0
	sf.written = 0
}

func (sf *Sftp) attempt() {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	sf.attempts++
}

func createProgressBar(maxBytes int64) *progressbar.ProgressBar {
	bar := progressbar.NewOptions64(maxBytes,
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(25),
		progressbar.OptionSetDescription("[cyan][reset] SFTP file syncing..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	return bar
}

func (sf *Sftp) write(reader io.Reader, pathGenerator storage.PathGeneratorFunc, offset int64) error {
	maxBytes := int64(-1)

	// Checking if reader is a file so we can set the size for progress bar
	if file, ok := reader.(*os.File); ok {
		if info, err := file.Stat(); err == nil {
			maxBytes = info.Size()
		}
	}

	bar := createProgressBar(maxBytes)

	// Try to resume the file transfer if reader is also a seeker and offset is greater than 0
	if seeker, ok := reader.(io.ReadSeeker); ok && offset > 0 {
		// File-based readers will maintain the read pointer.
		// So for those readers calling Read func after a failure will resume the read rather than read from start.
		// We still explicitly seek if the reader supports it and needs resuming as we are not sure which type of readers are passed to this func
		_, err := seeker.Seek(offset, 0)

		if err != nil {
			return fmt.Errorf("[sftp] fail to seek to offset %d: %v, %w", offset, err, ErrNonRetryable)
		}

		// Move the progress bar to the offset. If this fails, it's non-critical, so we just log the error.
		if err = bar.Add64(offset); err != nil {
			slog.Error("[sftp] fail to add offset to progress bar", slog.Any("error", err))
		}
	}

	conn, err := dialer.NewSsh(sf.SshHost, sf.SshKey, sf.SshUser).CreateSshClient()

	if err != nil {
		return fmt.Errorf("[sftp] fail to create ssh connection, error: %v", err)
	}

	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("[sftp] fail to close ssh connection", slog.Any("error", err))
		}
	}()

	client, err := sftpdialer.NewClient(conn)
	if err != nil {
		return err
	}

	defer func() {
		if err := client.Close(); err != nil {
			slog.Error("[sftp] fail to close sftp connection", slog.Any("error", err))
		}
	}()

	path := sf.Path
	if pathGenerator != nil {
		path = pathGenerator(sf.Path)
	}

	slog.Debug("[sftp] creating file via SFTP", slog.Any("path", path))

	var file *sftpdialer.File

	if offset > 0 {
		if file, err = client.OpenFile(path, os.O_WRONLY|os.O_APPEND); err != nil {
			return fmt.Errorf("[sftp] fail to open remote file %s, via SFTP, error: %v", path, err)
		}
	} else {
		if file, err = client.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC); err != nil {
			return fmt.Errorf("[sftp] fail to create remote file %s, via SFTP, error: %v", path, err)
		}
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("[sftp] fail to close sftp file", slog.Any("error", err))
		}
	}()

	n, err := io.Copy(io.MultiWriter(file, bar), reader)

	sf.mu.Lock()
	sf.written += n
	sf.mu.Unlock()

	return err
}

func (sf *Sftp) Save(reader io.Reader, pathGenerator storage.PathGeneratorFunc) error {
	sf.reset()

	for {
		err := sf.write(reader, pathGenerator, sf.written)

		// Contents have been saved properly, just return
		if err == nil {
			return nil
		}

		if errors.Is(err, ErrNonRetryable) {
			return err
		}

		if sf.MaxAttempts > 0 && sf.attempts >= sf.MaxAttempts {
			return fmt.Errorf("[sftp] save failed after %d attempts: %v", sf.MaxAttempts, err)
		}

		delay := time.Duration(math.Min(
			float64(BaseDelay*(1<<sf.attempts)),
			float64(1*time.Minute),
		))

		slog.Debug(fmt.Sprintf("[sftp] retry after %0.f seconds", delay.Seconds()))

		time.Sleep(delay)
		sf.attempt()
		slog.Debug("[sftp] retrying upload", slog.Int("attempt", sf.attempts), slog.Any("error", err))
	}
}

func (sf *Sftp) IsPathDir(path string) (bool, error) {
	conn, err := dialer.NewSsh(sf.SshHost, sf.SshKey, sf.SshUser).CreateSshClient()

	if err != nil {
		return false, fmt.Errorf("fail to create ssh connection, error: %v", err)
	}

	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("[sftp] fail to close ssh connection", slog.Any("error", err))
		}
	}()

	client, err := sftpdialer.NewClient(conn)
	if err != nil {
		return false, err
	}

	defer func() {
		if err := client.Close(); err != nil {
			slog.Error("[sftp] fail to close sftp connection", slog.Any("error", err))
		}
	}()

	destInfo, err := client.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, fmt.Errorf("[sftp] fail to get destination file info, error: %v", err)
	}

	return destInfo.IsDir(), nil
}

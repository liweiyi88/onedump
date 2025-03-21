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

var ErrNotRetryable = errors.New("error not retryable")

type Result struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error"`
	Written int64  `json:"written"`
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
	Result      Result
}

func NewSftp(maxAttempts int, path, sshHost, sshUser, sshKey string) *Sftp {
	return &Sftp{
		MaxAttempts: maxAttempts,
		Path:        path,
		SshHost:     sshHost,
		SshUser:     sshUser,
		SshKey:      sshKey,
	}
}

func (sf *Sftp) reset() {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	sf.Result = Result{}
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

	// Checking if reader is a file so we can set the size of progress bar
	if file, ok := reader.(*os.File); ok {
		if info, err := file.Stat(); err == nil {
			maxBytes = info.Size()
		}
	}

	bar := createProgressBar(maxBytes)

	// Try to resume the file transfer if reader is also a seeker and offset is greater than 0
	if seeker, ok := reader.(io.ReadSeeker); ok && offset > 0 {
		// File-based readers will maintain the read pointer.
		// So for those readers calling Read func after a failure will resume the read rather than read from 0.
		// We still explicitly seek if the reader supports it and needs resuming as we are not sure which type of readers are passed to this func
		_, err := seeker.Seek(offset, 0)

		if err != nil {
			return fmt.Errorf("failed to seek to offset %d: %v, %w", offset, err, ErrNotRetryable)
		}

		// move progress bar to offset as well
		bar.Add64(offset)
	}

	conn, err := dialer.NewSsh(sf.SshHost, sf.SshKey, sf.SshUser).CreateSshClient()

	if err != nil {
		return fmt.Errorf("fail to create ssh connection, error: %v", err)
	}

	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("fail to close ssh connection", slog.Any("error", err))
		}
	}()

	client, err := sftpdialer.NewClient(conn)
	if err != nil {
		return err
	}

	defer func() {
		if err := client.Close(); err != nil {
			slog.Error("fail to close sftp connection", slog.Any("error", err))
		}
	}()

	path := pathGenerator(sf.Path)

	var file *sftpdialer.File

	if offset > 0 {
		if file, err = client.OpenFile(path, os.O_WRONLY|os.O_APPEND); err != nil {
			return fmt.Errorf("fail to open remote file via SFTP, error: %v", err)
		}
	} else {
		if file, err = client.Create(path); err != nil {
			return fmt.Errorf("fail to create remote file via SFTP, error: %v", err)
		}
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("fail to close sftp file", slog.Any("error", err))
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
			sf.Result.OK = true
			sf.Result.Written = sf.written
			sf.Result.Error = ""
			return nil
		}

		if errors.Is(err, ErrNotRetryable) {
			sf.Result.OK = false
			sf.Result.Written = sf.written
			sf.Result.Error = err.Error()
			return err
		}

		if sf.MaxAttempts > 0 && sf.attempts >= sf.MaxAttempts {
			sf.Result.OK = false
			sf.Result.Written = sf.written
			sf.Result.Error = "reached max retries"
			return fmt.Errorf("failed after %d attempts: %v", sf.MaxAttempts, err)
		}

		delay := time.Duration(math.Min(
			float64(BaseDelay*(1<<sf.attempts)),
			float64(1*time.Minute),
		))

		slog.Info(fmt.Sprintf("retry after %0.f seconds", delay.Seconds()))

		time.Sleep(delay)
		sf.attempt()
		slog.Info("retrying upload", slog.Int("attempt", sf.attempts), slog.Any("error", err))
	}
}

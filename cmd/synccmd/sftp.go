package synccmd

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/liweiyi88/onedump/filesync"
	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/storage/sftp"
	"github.com/spf13/cobra"
)

var (
	source, destination, pattern, checksumFile string
	sftpHost, sftpUser, sftpKey                string
	sftpMaxAttempts                            int
	attach, verbose, checksum                  bool
)

func init() {
	SyncSftpCmd.Flags().StringVarP(&source, "source", "s", "", "the source file path to be transferred to the destination, supports folder as well (required)")
	SyncSftpCmd.Flags().StringVarP(&destination, "destination", "d", "", "the destination file path that we want to write to, supports folder as well (required)")
	SyncSftpCmd.Flags().BoolVar(&attach, "append", false, "if true, re-run the command will try to append content to file instead of creating a new file. (optional)")
	SyncSftpCmd.Flags().StringVar(&sftpHost, "ssh-host", "", "the remote SSH host (required)")
	SyncSftpCmd.Flags().StringVar(&sftpUser, "ssh-user", "", "the remote SSH user (required)")
	// Pass encoded private key content via base64. e.g. MacOS: base64 < ~/.ssh/id_rsa
	// Or just pass the private key filename.
	SyncSftpCmd.Flags().StringVar(&sftpKey, "ssh-key", "", "the base64 encoded ssh private key content or the ssh private key file path or the raw private content (required)")
	SyncSftpCmd.Flags().IntVar(&sftpMaxAttempts, "max-attempts", 0, "the maximum number of retries if an error is encountered; by default, retries are unlimited (optional)")
	SyncSftpCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "prints additional debug information (optional)")
	SyncSftpCmd.Flags().BoolVar(&checksum, "checksum", false, "whether to save the checksum to avoid repeating file transfers, default false (optional)")
	SyncSftpCmd.Flags().StringVar(&checksumFile, "checksum-file", "", "save checksum results in a specific file if --checksum=true, default: /path/to/sync/folder/checksum.onedump (optional)")
	SyncSftpCmd.Flags().StringVarP(&pattern, "pattern", "p", "", "only read files that follow the same pattern, for example binlog.* (optional)")

	SyncSftpCmd.MarkFlagRequired("source")
	SyncSftpCmd.MarkFlagRequired("destination")
	SyncSftpCmd.MarkFlagRequired("ssh-host")
	SyncSftpCmd.MarkFlagRequired("ssh-user")
	SyncSftpCmd.MarkFlagRequired("ssh-key")
}

func syncFiles(sources []string, destination string, checksum bool, checksumFile string, isDestinationDir bool, config *sftp.SftpConifg) error {
	errCh := make(chan error, len(sources))

	// Process maximum 10 files at a time
	semaphore := make(chan struct{}, 10)

	var wg sync.WaitGroup
	for _, file := range sources {
		wg.Add(1)
		semaphore <- struct{}{}
		go func() {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			if err := syncFile(file, destination, checksum, checksumFile, isDestinationDir, config); err != nil {
				errCh <- err
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var allErrors []error
	for err := range errCh {
		allErrors = append(allErrors, err)
	}

	return errors.Join(allErrors...)
}

func syncFile(source, destination string, checksum bool, checksumFile string, isDestinationDir bool, config *sftp.SftpConifg) error {
	syncFunc := func() error {
		sourceFile, err := os.Open(source)

		if err != nil {
			return fmt.Errorf("fail to open the source file: %s, error: %v", source, err)
		}

		defer func() {
			if err := sourceFile.Close(); err != nil {
				slog.Error("fail to close the source file", slog.Any("error", err))
			}
		}()

		path := destination
		if isDestinationDir {
			sourceFileInfo, err := sourceFile.Stat()
			if err != nil {
				return fmt.Errorf("fail to get source file stat, error: %v", err)
			}

			path = filepath.Join(destination, sourceFileInfo.Name())
		}

		sftp := sftp.NewSftp(config)
		err = sftp.Save(sourceFile, func(filename string) string { return path })

		if err != nil {
			return fmt.Errorf("fail to sync file %s to destination %s, error: %v", source, destination, err)
		}

		return nil
	}

	fs := filesync.NewFileSync(checksum, checksumFile)

	return fs.SyncFile(source, syncFunc)
}

var SyncSftpCmd = &cobra.Command{
	Use:   "sftp",
	Short: "Resumable and concurrent SFTP files transfer.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if verbose {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		config := &sftp.SftpConifg{
			Host:        sftpHost,
			User:        sftpUser,
			Key:         sftpKey,
			MaxAttempts: sftpMaxAttempts,
		}

		if config.Host == "" || config.User == "" || config.Key == "" {
			return errors.New("ssh host, user, and key are required for SFTP connection")
		}

		isDestinationDir, err := sftp.NewSftp(config).IsPathDir(destination)
		if err != nil {
			return fmt.Errorf("fail to check if destination is directory, error: %v", err)
		}

		sourceInfo, err := os.Stat(source)
		if err != nil {
			return fmt.Errorf("fail to get source info %v", err)
		}

		if sourceInfo.IsDir() && !isDestinationDir {
			return errors.New("detination should not be a file when transfer multiple files from the source")
		}

		if sourceInfo.IsDir() {
			files, err := fileutil.ListFiles(source, pattern, "")
			if err != nil {
				return fmt.Errorf("fail to list all files for source dir, error: %v", err)
			}

			if len(files) == 0 {
				return errors.New("no file found for syncing")
			}

			return syncFiles(files, destination, checksum, checksumFile, isDestinationDir, config)
		}

		return syncFile(source, destination, checksum, checksumFile, isDestinationDir, config)
	},
}

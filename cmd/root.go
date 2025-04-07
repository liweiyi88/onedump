package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/handler"
	"github.com/liweiyi88/onedump/slow"
	"github.com/liweiyi88/onedump/storage/s3"
)

var file, s3Bucket, s3Prefix, s3Region, s3AccessKeyId, s3SecretAccessKey, cron string
var sloglog, database, pattern, source, destination string
var limit int
var mask, attach, verbose bool

var sftpHost, sftpUser, sftpKey string
var checksum bool
var sftpMaxAttempts int

var rootCmd = &cobra.Command{
	Use:   "onedump",
	Short: "Dump database content from different sources to different destinations with a yaml config file.",
	Args:  cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		_, stop := context.WithCancel(context.Background())
		appSignal := make(chan os.Signal, 3)
		signal.Notify(appSignal, os.Interrupt, syscall.SIGTERM)

		defer func() {
			stop()
		}()

		content, err := getConfigContent()
		if err != nil {
			return fmt.Errorf("failed to read job file from %s, error: %v", file, err)
		}

		var oneDump config.Dump
		err = yaml.Unmarshal(content, &oneDump)
		if err != nil {
			return fmt.Errorf("failed to read job content from %s, error: %v", file, err)
		}

		err = oneDump.Validate()
		if err != nil {
			return fmt.Errorf("invalid job configuration, error: %v", err)
		}

		numberOfJobs := len(oneDump.Jobs)
		if numberOfJobs == 0 {
			return fmt.Errorf("no job is defined in the file %s", file)
		}

		if cron == "" {
			return handler.NewDumpHandler(&oneDump).Do()
		} else {
			d, err := time.ParseDuration(cron)
			if err != nil {
				return fmt.Errorf("invalid job's interval time duration, error: %v", err)
			}

			scheduler := gocron.NewScheduler(time.UTC)
			job, err := scheduler.Every(d).Do(handler.NewDumpHandler(&oneDump).Do)
			if err != nil {
				return fmt.Errorf("failed to specify the job func, error: %v", err)
			}

			var jobErr error
			job.RegisterEventListeners(gocron.WhenJobReturnsError(func(jobName string, err error) {
				go func() {
					jobErr = err
					// Stop the scheduler if job has an error.
					scheduler.Stop()
				}()
			}))

			go func() {
				<-appSignal
				stop()
				// graceful shutdown
				scheduler.Stop()
			}()

			scheduler.StartBlocking()
			return jobErr
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func getConfigContent() ([]byte, error) {
	if s3Bucket != "" {
		s3Client := s3.NewS3(s3Bucket, file, s3Region, s3AccessKeyId, s3SecretAccessKey)
		return s3Client.GetContent()
	} else {
		return os.ReadFile(file)
	}
}

func init() {
	rootCmd.Flags().StringVarP(&file, "file", "f", "", "jobs yaml file path.")
	rootCmd.MarkFlagRequired("file")

	rootCmd.Flags().StringVarP(&cron, "cron", "c", "", "run onedump with cron mode by passing cron experssions. e.g. --cron '1h' (optional)")
	rootCmd.Flags().StringVarP(&s3Bucket, "s3-bucket", "b", "", "read config file from a s3 bucket (optional)")
	rootCmd.Flags().StringVarP(&s3Region, "aws-region", "r", "", "the aws region to read the config file (optional)")
	rootCmd.Flags().StringVarP(&s3AccessKeyId, "aws-key", "k", "", "aws access key id to overwrite the default one. (optional)")
	rootCmd.Flags().StringVarP(&s3SecretAccessKey, "aws-secret", "s", "", "aws secret access key to overwrite the default one. (optional)")

	slowCmd.Flags().StringVarP(&sloglog, "file", "f", "", "path to the slow log file. a directory can also be specified. (required)")
	slowCmd.Flags().StringVarP(&database, "database", "d", string(slow.MySQL), "specify the database engine (optional)")
	slowCmd.Flags().StringVarP(&pattern, "pattern", "p", "", "only read files that follow the same pattern, for example *slow.log . (optional)")
	slowCmd.Flags().IntVarP(&limit, "limit", "l", 0, "limit the number of results. no limit is set by default. (optional)")
	slowCmd.Flags().BoolVarP(&mask, "mask", "m", true, "mask query values. enabled by default. (optional)")
	slowCmd.MarkFlagRequired("file")

	syncSftpCmd.Flags().StringVarP(&source, "source", "s", "", "the source file path to be transferred to the destination, supports folder as well (required)")
	syncSftpCmd.Flags().StringVarP(&destination, "destination", "d", "", "the destination file path that we want to write to, supports folder as well (required)")
	syncSftpCmd.Flags().BoolVar(&attach, "append", false, "if true, re-run the command will try to append content to file instead of creating a new file. (optional)")
	syncSftpCmd.Flags().StringVar(&sftpHost, "ssh-host", "", "the remote SSH host (required)")
	syncSftpCmd.Flags().StringVar(&sftpUser, "ssh-user", "", "the remote SSH user (required)")
	// Pass encoded private key content via base64. e.g. MacOS: base64 < ~/.ssh/id_rsa
	// Or just pass the private key filename.
	syncSftpCmd.Flags().StringVar(&sftpKey, "ssh-key", "", "the base64 encoded ssh private key content or the ssh private key file path or the raw private content (required)")
	syncSftpCmd.Flags().IntVar(&sftpMaxAttempts, "max-attempts", 0, "the maximum number of retries if an error is encountered; by default, retries are unlimited (optional)")
	syncSftpCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "prints additional debug information (optional)")
	syncSftpCmd.Flags().BoolVar(&checksum, "checksum", false, "whether to save the checksum to avoid repeating file transfers, default false (optional)")
	syncSftpCmd.Flags().StringVarP(&pattern, "pattern", "p", "", "only read files that follow the same pattern, for example binlog.* (optional)")

	syncSftpCmd.MarkFlagRequired("source")
	syncSftpCmd.MarkFlagRequired("destination")
	syncSftpCmd.MarkFlagRequired("ssh-host")
	syncSftpCmd.MarkFlagRequired("ssh-user")
	syncSftpCmd.MarkFlagRequired("ssh-key")

	// The command also needs the DATABASE_URL, AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY as env var
	binlogSyncS3Cmd.Flags().StringVarP(&s3Bucket, "s3-bucket", "b", "", "AWS S3 bucket name that used for saving binlog files")
	binlogSyncS3Cmd.Flags().StringVarP(&s3Prefix, "s3-prefix", "p", "", "AWS S3 file prefix (folder) that used for saving binlog files")
	binlogSyncS3Cmd.MarkFlagRequired("s3-bucket")
	binlogSyncS3Cmd.MarkFlagRequired("s3-prefix")
	binlogSyncS3Cmd.Flags().BoolVar(&checksum, "checksum", false, "whether to save the checksum to avoid repeating file transfers, default false (optional)")
	binlogSyncS3Cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "prints additional debug information (optional)")

	rootCmd.AddCommand(slowCmd)
	rootCmd.AddCommand(syncSftpCmd)
	rootCmd.AddCommand(binlogSyncS3Cmd)
}

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/liweiyi88/onedump/cmd/binlogcmd"
	"github.com/liweiyi88/onedump/cmd/downloadcmd"
	"github.com/liweiyi88/onedump/cmd/slowcmd"
	"github.com/liweiyi88/onedump/cmd/synccmd"
	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/handler"
	"github.com/liweiyi88/onedump/storage/s3"
)

var file, s3Bucket, s3Region, s3AccessKeyId, s3SecretAccessKey, s3SessionToken, cron string
var verbose bool

var RootCmd = &cobra.Command{
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

		if verbose {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		content, err := getConfigContent()
		if err != nil {
			return fmt.Errorf("failed to read job file from %s, error: %v", file, err)
		}

		oneDump := config.Dump{
			MaxJobs: config.DefaultMaxConcurrentJobs,
		}

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
	if err := RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func getConfigContent() ([]byte, error) {
	if s3Bucket != "" {
		s3Client := s3.NewS3(s3Bucket, file, s3Region, s3AccessKeyId, s3SecretAccessKey, s3SessionToken)
		return s3Client.GetContent(context.Background())
	} else {
		return os.ReadFile(file)
	}
}

func init() {
	RootCmd.Flags().StringVarP(&file, "file", "f", "", "jobs yaml file path.")
	RootCmd.MarkFlagRequired("file")

	RootCmd.Flags().StringVarP(&cron, "cron", "c", "", "run onedump with cron mode by passing cron experssions. e.g. --cron '1h' (optional)")
	RootCmd.Flags().StringVarP(&s3Bucket, "s3-bucket", "b", "", "read config file from a s3 bucket (optional)")
	RootCmd.Flags().StringVarP(&s3Region, "aws-region", "r", "", "the aws region to read the config file (optional)")
	RootCmd.Flags().StringVarP(&s3AccessKeyId, "aws-key", "k", "", "aws access key id to overwrite the default one. (optional)")
	RootCmd.Flags().StringVarP(&s3SecretAccessKey, "aws-secret", "s", "", "aws secret access key to overwrite the default one. (optional)")
	RootCmd.Flags().StringVarP(&s3SessionToken, "aws-session-token", "t", "", "specify the aws session token if you use a temporary credentials. (optional)")
	RootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "prints additional debug information (optional)")

	RootCmd.AddCommand(slowcmd.SlowCmd)
	RootCmd.AddCommand(synccmd.SyncCmd)
	RootCmd.AddCommand(binlogcmd.BinlogCmd)
	RootCmd.AddCommand(downloadcmd.DownloadCmd)
}

package cmd

import (
	"fmt"
	"os"

	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/runner"
	"github.com/liweiyi88/onedump/storage/s3"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var file, s3Bucket, s3Region, s3AccessKeyId, s3SecretAccessKey string

var rootCmd = &cobra.Command{
	Use:   "-f /path/to/jobs.yaml",
	Short: "Dump database content from different sources to different destinations with a yaml config file.",
	Args:  cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
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

		runner := runner.NewDumpRunner(&oneDump)
		return runner.Do()
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

	rootCmd.Flags().StringVarP(&s3Bucket, "s3-bucket", "b", "", "read config file from a s3 bucket (optional)")
	rootCmd.Flags().StringVarP(&s3Region, "aws-region", "r", "", "the aws region to read the config file (optional)")
	rootCmd.Flags().StringVarP(&s3AccessKeyId, "aws-key", "k", "", "aws access key id to overwrite the default one. (optional)")
	rootCmd.Flags().StringVarP(&s3SecretAccessKey, "aws-secret", "s", "", "aws secret access key to overwrite the default one. (optional)")
}

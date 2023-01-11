package cmd

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/liweiyi88/onedump/dump"
	"github.com/liweiyi88/onedump/storage/s3"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var file, s3Bucket, s3Region, s3AccessKeyId, s3SecretAccessKey string

var rootCmd = &cobra.Command{
	Use:   "-f /path/to/jobs.yaml",
	Short: "Dump database content from different sources to different destinations with a yaml config file.",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		content, err := getConfigContent()
		if err != nil {
			log.Fatalf("failed to read job file from %s, error: %v", file, err)
		}

		var oneDump dump.Dump
		err = yaml.Unmarshal(content, &oneDump)
		if err != nil {
			log.Fatalf("failed to read job content from %s, error: %v", file, err)
		}

		err = oneDump.Validate()
		if err != nil {
			log.Fatalf("invalid job configuration, error: %v", err)
		}

		numberOfJobs := len(oneDump.Jobs)
		if numberOfJobs == 0 {
			log.Printf("no job is defined in the file %s", file)
			return
		}

		resultCh := make(chan *dump.JobResult)

		for _, job := range oneDump.Jobs {
			go func(job *dump.Job, resultCh chan *dump.JobResult) {
				resultCh <- job.Run()
			}(job, resultCh)
		}

		var wg sync.WaitGroup
		wg.Add(numberOfJobs)
		go func(resultCh chan *dump.JobResult) {
			for result := range resultCh {
				fmt.Println(result.String())
				wg.Done()
			}
		}(resultCh)

		wg.Wait()
		close(resultCh)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
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
	rootCmd.Flags().StringVarP(&s3Region, "s3-region", "r", "", "the s3 region to read the config file (optional)")
	rootCmd.Flags().StringVarP(&s3AccessKeyId, "s3-key", "k", "", "s3 access key id to overwrite the default one. (optional)")
	rootCmd.Flags().StringVarP(&s3SecretAccessKey, "s3-secret", "s", "", "s3 secret access key to overwrite the default one. (optional)")
}

package cmd

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/liweiyi88/onedump/dump"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var file string

var rootCmd = &cobra.Command{
	Use:   "-f /path/to/jobs.yaml",
	Short: "Dump database content from different sources to different destinations with a yaml config file.",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		content, err := os.ReadFile(file)
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
				result.Print()
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

func init() {
	rootCmd.Flags().StringVarP(&file, "file", "f", "", "jobs yaml file path.")
	rootCmd.MarkFlagRequired("file")
}

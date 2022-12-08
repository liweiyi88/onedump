package cmd

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/liweiyi88/onedump/dumpjob"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	file string
)

var applyCmd = &cobra.Command{
	Use:   "apply -f /path/to/jobs.yaml",
	Args:  cobra.ExactArgs(0),
	Short: "Dump db content from different sources to diferent destinations",
	Run: func(cmd *cobra.Command, args []string) {
		content, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("failed to read job file from %s, error: %v", file, err)
		}

		var oneDump dumpjob.OneDump
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

		resultCh := make(chan *dumpjob.JobResult)

		for _, job := range oneDump.Jobs {
			go func(job dumpjob.Job, resultCh chan *dumpjob.JobResult) {
				resultCh <- job.Run()
			}(job, resultCh)
		}

		var wg sync.WaitGroup
		wg.Add(numberOfJobs)
		go func(resultCh chan *dumpjob.JobResult) {
			for result := range resultCh {
				if result.Error != nil {
					fmt.Printf("Job %s failed, it took %s with error: %v \n", result.JobName, result.Elapsed, result.Error)
				} else {
					fmt.Printf("Job %s succeeded and it took %v \n", result.JobName, result.Elapsed)
				}
				wg.Done()
			}
		}(resultCh)

		wg.Wait()
		close(resultCh)
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)
	applyCmd.Flags().StringVarP(&file, "file", "f", "", "jobs yaml file path (required)")
	applyCmd.MarkFlagRequired("file")
}

package jobresult

import (
	"fmt"
	"time"
)

type JobResult struct {
	Error   error
	JobName string
	Elapsed time.Duration
}

func (result *JobResult) String() string {
	if result.Error != nil {
		return fmt.Sprintf("%s failed, it took %s with error: %v", result.JobName, result.Elapsed, result.Error)
	}

	return fmt.Sprintf("%s succeeded, it took %v", result.JobName, result.Elapsed)
}

func (result *JobResult) ToSlackText() string {
	if result.Error != nil {
		return fmt.Sprintf(":x: `%s` failed, it took *%s* ```%v```", result.JobName, result.Elapsed, result.Error)
	}

	return fmt.Sprintf(":white_check_mark: `%s` succeeded, it took *%v*", result.JobName, result.Elapsed)
}

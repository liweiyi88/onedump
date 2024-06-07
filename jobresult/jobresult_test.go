package jobresult

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {
	jr := JobResult{
		Error:   errors.New("job error"),
		JobName: "fail job",
		Elapsed: time.Second,
	}

	expect := fmt.Sprintf("%s failed, it took %s with error: %v", jr.JobName, jr.Elapsed, jr.Error)
	assert.Equal(t, expect, jr.String())

	jr.Error = nil
	expect = fmt.Sprintf("%s succeeded, it took %v", jr.JobName, jr.Elapsed)
	assert.Equal(t, expect, jr.String())
}

func TestToSlackText(t *testing.T) {
	jr := JobResult{
		Error:   errors.New("job error"),
		JobName: "fail job",
		Elapsed: time.Second,
	}

	expect := fmt.Sprintf(":x: `%s` failed, it took *%s* ```%v```", jr.JobName, jr.Elapsed, jr.Error)

	assert.Equal(t, expect, jr.ToSlackText())

	jr.Error = nil
	expect = fmt.Sprintf(":white_check_mark: `%s` succeeded, it took *%v*", jr.JobName, jr.Elapsed)
	assert.Equal(t, expect, jr.ToSlackText())
}

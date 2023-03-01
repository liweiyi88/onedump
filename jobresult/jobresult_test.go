package jobresult

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestString(t *testing.T) {
	jr := JobResult{
		Error:   errors.New("job error"),
		JobName: "fail job",
		Elapsed: time.Second,
	}

	expect := fmt.Sprintf("%s failed, it took %s with error: %v", jr.JobName, jr.Elapsed, jr.Error)
	if jr.String() != expect {
		t.Errorf("expect %s, but actual got: %s", expect, jr.String())
	}

	jr.Error = nil
	expect = fmt.Sprintf("%s succeeded, it took %v", jr.JobName, jr.Elapsed)
	if jr.String() != expect {
		t.Errorf("expect %s, but actual got: %s", expect, jr.String())
	}
}

func TestToSlackText(t *testing.T) {
	jr := JobResult{
		Error:   errors.New("job error"),
		JobName: "fail job",
		Elapsed: time.Second,
	}

	expect := fmt.Sprintf(":x: `%s` failed, it took *%s* ```%v```", jr.JobName, jr.Elapsed, jr.Error)
	if jr.ToSlackText() != expect {
		t.Errorf("expect %s, but actual got: %s", expect, jr.ToSlackText())
	}

	jr.Error = nil
	expect = fmt.Sprintf(":white_check_mark: `%s` succeeded, it took *%v*", jr.JobName, jr.Elapsed)
	if jr.ToSlackText() != expect {
		t.Errorf("expect %s, but actual got: %s", expect, jr.ToSlackText())
	}
}

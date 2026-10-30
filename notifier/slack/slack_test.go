package slack

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liweiyi88/onedump/jobresult"
	"github.com/stretchr/testify/assert"
)

func TestNotify(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprintln(w, "done")
		assert.NoError(t, err)
	})

	mux.HandleFunc("/failed", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, err := fmt.Fprintln(w, "done")
		assert.NoError(t, err)
	})

	svr := httptest.NewServer(mux)
	defer svr.Close()

	slack := &Slack{
		IncomingWebhook: svr.URL,
	}

	results := make([]*jobresult.JobResult, 0, 2)

	err := slack.Notify(results)
	assert.Nil(t, err)

	results = append(results, &jobresult.JobResult{
		JobName: "success job",
		Elapsed: time.Second,
	})

	results = append(results, &jobresult.JobResult{
		Error:   errors.New("failed dump job"),
		JobName: "failed job",
		Elapsed: time.Second,
	})

	err = slack.Notify(results)
	assert.Nil(t, err)

	slack.IncomingWebhook = svr.URL + "/failed"

	err = slack.Notify(results)
	assert.NotNil(t, err)
}

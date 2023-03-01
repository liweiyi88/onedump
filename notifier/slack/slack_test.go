package slack

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liweiyi88/onedump/jobresult"
)

func TestNotify(t *testing.T) {

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "done")
	})

	mux.HandleFunc("/failed", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, "done")
	})

	svr := httptest.NewServer(mux)
	defer svr.Close()

	slack := &Slack{
		IncomingWebhook: svr.URL,
	}

	results := make([]*jobresult.JobResult, 0, 2)

	err := slack.Notify(results)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}

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
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}

	slack.IncomingWebhook = svr.URL + "/failed"

	err = slack.Notify(results)
	if err == nil {
		t.Error("expected error but got nil")
	}

	// text := &SlackMessage{
	// 	Text:  "hello my world",
	// 	Extra: "abcd",
	// }

	// data, err := json.Marshal(text)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// bytesReader := strings.NewReader(string(data))

	// client := &http.Client{}
	// res, err := client.Post(incomingwebhook, "application/json", bytesReader)

	// if res.StatusCode != http.StatusOK {

	// 	body, _ := io.ReadAll(res.Body)

	// 	fmt.Println(string(body))
	// }

	// if err != nil {
	// 	t.Fatal(err)
	// }
}

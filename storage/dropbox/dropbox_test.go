package dropbox

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSuccessfulSave(t *testing.T) {
	response := "{\"session_id\":\"123jlsdfdsfjksjdkf\"}"

	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, response)
	})

	mux.HandleFunc("/append_v2", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, nil)
	})

	mux.HandleFunc("/finish", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, nil)
	})

	svr := httptest.NewServer(mux)
	defer svr.Close()

	originMaxUpload := maxUpload
	maxUpload = 4

	originUploadSessionEndpoint := uploadSessionEndpoint
	uploadSessionEndpoint = svr.URL + "/start"

	originUploadSessionAppendEndpoint := uploadSessionAppendEndpoint
	uploadSessionAppendEndpoint = svr.URL + "/append_v2"

	originUploadSessionFinish := uploadSessionFinishEndpoint
	uploadSessionFinishEndpoint = svr.URL + "/finish"

	defer func() {
		uploadSessionEndpoint = originUploadSessionEndpoint
		uploadSessionAppendEndpoint = originUploadSessionAppendEndpoint
		uploadSessionFinishEndpoint = originUploadSessionFinish
		maxUpload = originMaxUpload
	}()

	dropbox := &Dropbox{}

	sr := strings.NewReader("file upload")
	err := dropbox.Save(sr, true, true)
	if err != nil {
		t.Errorf("expected err to be nil got %v", err)
	}
}

func TestUploadSessionFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, nil)
	})

	server1 := httptest.NewServer(mux)
	defer func() {
		server1.Close()
	}()

	originUploadSessionEndpoint := uploadSessionEndpoint
	uploadSessionEndpoint = server1.URL + "/start"

	defer func() {
		uploadSessionEndpoint = originUploadSessionEndpoint
	}()

	dropbox := &Dropbox{}

	sr := strings.NewReader("file upload")
	err := dropbox.Save(sr, true, true)

	if err == nil {
		t.Error("expected error but got nil error")
	}
}

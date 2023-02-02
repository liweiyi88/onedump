package dropbox

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHasTokenExpired(t *testing.T) {
	d := &Dropbox{
		RefreshToken: "refresh_token",
		ClientId:     "clientid",
		ClientSecret: "clientsecret",
	}

	expired := d.hasTokenExpired()
	if expired != true {
		t.Errorf("default expiredIn should indicate expired, but actual got not expired.")
	}

	d.expiredAt = time.Now().Add((expiredGap + 1) * time.Second)
	if d.hasTokenExpired() {
		t.Errorf("expected not exipre token but got expired.")
	}

	d.expiredAt = time.Now().Add(expiredGap * time.Second)
	t.Log("expiredAt", d.expiredAt)
	if !d.hasTokenExpired() {
		t.Errorf("expected expired token but got not expired.")
	}
}

func TestSuccessfulSave(t *testing.T) {
	response := "{\"session_id\":\"123jlsdfdsfjksjdkf\"}"

	mux := http.NewServeMux()

	mux.HandleFunc("/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "{\"access_token\":\"sl.BYBntuwSqTes9FsYOrJ68Hi_UvEDH5cZzqt3QSJ3fvVAz\",\"token_type\":\"bearer\",\"expires_in\":14400}")
	})

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

	originOauthTokenEndpoint := oauthTokenEndpoint
	oauthTokenEndpoint = svr.URL + "/oauth2/token"

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
		oauthTokenEndpoint = originOauthTokenEndpoint
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

	server := httptest.NewServer(mux)
	defer func() {
		server.Close()
	}()

	originUploadSessionEndpoint := uploadSessionEndpoint
	uploadSessionEndpoint = server.URL + "/start"

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

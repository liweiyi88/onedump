package dropbox

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liweiyi88/onedump/storage"
	"github.com/stretchr/testify/assert"
)

func TestHasTokenExpired(t *testing.T) {
	assert := assert.New(t)
	d := &Dropbox{
		RefreshToken: "refresh_token",
		ClientId:     "clientid",
		ClientSecret: "clientsecret",
	}

	expired := d.hasTokenExpired()
	assert.True(expired)

	d.expiredAt = time.Now().Add((expiredGap + 1) * time.Second)
	assert.False(d.hasTokenExpired())

	d.expiredAt = time.Now().Add(expiredGap * time.Second)
	assert.True(d.hasTokenExpired())
}

func TestGetAccessToken(t *testing.T) {
	assert := assert.New(t)
	mux := http.NewServeMux()

	mux.HandleFunc("/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "{\"access_token\":\"sl.BYBntuwSqTes9FsYOrJ68Hi_UvEDH5cZzqt3QSJ3fvVAz\",\"token_type\":\"bearer\",\"expires_in\":14400}")
	})

	mux.HandleFunc("/oauth2/token-wrongjson", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "not json")
	})

	svr := httptest.NewServer(mux)
	defer svr.Close()

	originOauthTokenEndpoint := oauthTokenEndpoint
	oauthTokenEndpoint = svr.URL + "/oauth2/token"

	defer func() {
		oauthTokenEndpoint = originOauthTokenEndpoint
	}()

	dropbox := &Dropbox{}

	assert.Nil(dropbox.getAccessToken())
	assert.Equal("sl.BYBntuwSqTes9FsYOrJ68Hi_UvEDH5cZzqt3QSJ3fvVAz", dropbox.accessToken)

	oauthTokenEndpoint = svr.URL + "/oauth2/token-wrongjson"
	assert.NotNil(dropbox.getAccessToken())
}

func TestSaveSuccess(t *testing.T) {
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
	err := dropbox.Save(sr, storage.PathGenerator(true, true))

	assert.Nil(t, err)
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

	err := dropbox.Save(sr, storage.PathGenerator(true, true))
	assert.NotNil(t, err)
}

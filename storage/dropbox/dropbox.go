package dropbox

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/liweiyi88/onedump/storage"
)

var (
	oauthTokenEndpoint          = "https://api.dropboxapi.com/oauth2/token"
	uploadSessionEndpoint       = "https://content.dropboxapi.com/2/files/upload_session/start"
	uploadSessionAppendEndpoint = "https://content.dropboxapi.com/2/files/upload_session/append_v2"
	uploadSessionFinishEndpoint = "https://content.dropboxapi.com/2/files/upload_session/finish"
)

const (
	B        = 1
	KB int64 = 1 << (10 * iota)
	MB
	GB
	TB
)

// Expire the access token before it reaches the real expiry to avoid edge cases.
const expiredGap = 10

// Dropb limits of file upload per api call.
var maxUpload = 150 * MB

type uploadSessionParam struct {
	Close bool `json:"close"`
}

type oauthTokenResponse struct {
	AccessToken string `json:"access_token"` // The access token to be used to call the Dropbox API.
	TokenType   string `json:"token_type"`
	ExpiresIn   uint   `json:"expires_in"` // The length of time in seconds that the access token will be valid for.
	Uid         string `json:"uid"`
	AccountId   string `json:"account_id"`
}

type Cursor struct {
	Offset    int    `json:"offset"`
	SessionId string `json:"session_id"`
}

type Commit struct {
	Path           string `json:"path,omitempty"`
	Mode           string `json:"mode,omitempty"`
	Autorename     bool   `json:"autorename"`
	ClientModified string `json:"client_modified,omitempty"`
	Mute           string `json:"mute,omitempty"`
	StrictConflict bool   `json:"strict_conflict"`
	ContentHash    string `json:"content_hash,omitempty"`
}

type uploadSessionFinishParam struct {
	Cursor Cursor `json:"cursor"`
	Commit Commit `json:"commit"`
}

type uploadSessionAppendParam struct {
	Close  bool   `json:"close"`
	Cursor Cursor `json:"cursor"`
}

type uploadSessionResponse struct {
	SessionId string `json:"session_id"`
}

type Dropbox struct {
	accessToken  string
	expiredAt    time.Time
	Path         string `yaml:"path"`
	RefreshToken string `yaml:"refreshtoken"`
	ClientId     string `yaml:"clientid"`
	ClientSecret string `yaml:"clientsecret"`
}

func (dropbox *Dropbox) Save(reader io.Reader, pathGenerator storage.PathGeneratorFunc) error {
	offset := 0
	sessionId := ""
	buf := make([]byte, maxUpload)
	client := &http.Client{}

	for {
		// We have to use io.ReadFull, otherwise reader.Read(buf) won't be able to read contents to the full length of buf.
		// Which will cause error for uploading.
		n, readErr := io.ReadFull(reader, buf)
		buf = buf[:n]

		if readErr != nil {
			if readErr == io.EOF {
				break
			}

			if readErr != io.ErrUnexpectedEOF {
				return fmt.Errorf("failed to read from reader :%v", readErr)
			}
		}

		if n > 0 {
			if sessionId == "" {
				sessId, err := dropbox.startUploadSession(client)
				if err != nil {
					return err
				}

				slog.Info("started dropbox upload session", slog.Any("sessionId", sessId))
				sessionId = sessId
			}

			if int64(len(buf)) < maxUpload {
				err := dropbox.uploadSessionFinish(client, buf, offset, sessionId, pathGenerator)
				if err != nil {
					return err
				}

				slog.Info("finish dropbox upload session with offset", slog.Any("offset", offset))

				offset += n
				continue
			}

			err := dropbox.uploadSessionAppend(client, buf, offset, sessionId)
			if err != nil {
				return err
			}

			slog.Info("append dropbox upload session with offset", slog.Any("offset", offset))
			offset += n
		}
	}

	return nil
}

func (dropbox *Dropbox) getAccessToken() error {
	data := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {dropbox.RefreshToken},
		"client_id":     {dropbox.ClientId},
		"client_secret": {dropbox.ClientSecret},
	}

	res, err := http.PostForm(oauthTokenEndpoint, data)
	if err != nil {
		return fmt.Errorf("failed to request dropbox oauth token: %v", err)
	}

	defer func() {
		if err := res.Body.Close(); err != nil {
			slog.Error("could not close response body", slog.Any("error", err))
		}
	}()

	var tokenResponse oauthTokenResponse

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("request %s is not successful, get status code: %d, body: %s", oauthTokenEndpoint, res.StatusCode, string(body))
	}

	if err = json.Unmarshal(body, &tokenResponse); err != nil {
		return fmt.Errorf("could not unmarshal dropbox oauth token response :%v", err)
	}

	dropbox.accessToken = tokenResponse.AccessToken
	dropbox.expiredAt = time.Now().Add(time.Duration(tokenResponse.ExpiresIn) * time.Second)
	return nil
}

func (dropbox *Dropbox) hasTokenExpired() bool {
	if dropbox.expiredAt.IsZero() {
		return true
	}

	expireTime := dropbox.expiredAt.Add(-time.Second * expiredGap)

	return time.Now().After(expireTime) || time.Now().Equal(expireTime)
}

func (dropbox *Dropbox) startUploadSession(client *http.Client) (string, error) {
	param := uploadSessionParam{
		Close: false,
	}

	body, err := dropbox.sendRequest(client, "POST", uploadSessionEndpoint, nil, param)
	if err != nil {
		return "", fmt.Errorf("failed to send upload session start request %v", err)
	}

	sessionResponse := &uploadSessionResponse{}

	if err = json.Unmarshal(body, sessionResponse); err != nil {
		return "", fmt.Errorf("could not unmarshal upload session response :%v", err)
	}

	return sessionResponse.SessionId, nil
}

func (dropbox *Dropbox) uploadSessionFinish(client *http.Client, data []byte, offset int, sessionId string, pathGenerator storage.PathGeneratorFunc) error {
	bytesReader := bytes.NewReader(data)
	path := pathGenerator(dropbox.Path)
	param := uploadSessionFinishParam{
		Commit: Commit{
			Path: path,
			Mode: "overwrite",
		},
		Cursor: Cursor{
			Offset:    offset,
			SessionId: sessionId,
		},
	}

	_, err := dropbox.sendRequest(client, "POST", uploadSessionFinishEndpoint, bytesReader, param)

	return err
}

func (dropbox *Dropbox) uploadSessionAppend(client *http.Client, data []byte, offset int, sessionId string) error {
	bytesReader := bytes.NewReader(data)
	param := uploadSessionAppendParam{
		Close: false,
		Cursor: Cursor{
			Offset:    offset,
			SessionId: sessionId,
		},
	}

	_, err := dropbox.sendRequest(client, "POST", uploadSessionAppendEndpoint, bytesReader, param)
	return err
}

func (dropbox *Dropbox) sendRequest(client *http.Client, method string, url string, data io.Reader, param any) ([]byte, error) {
	if dropbox.accessToken == "" || dropbox.hasTokenExpired() {
		if err := dropbox.getAccessToken(); err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	paramJson, err := json.Marshal(param)
	if err != nil {
		return nil, fmt.Errorf("could not encode param into json %v", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+dropbox.accessToken)
	req.Header.Set("Dropbox-API-Arg", string(paramJson))

	response, err := client.Do(req)

	if err != nil {
		return nil, fmt.Errorf("failed to send dropbox request %v", err)
	}

	defer func() {
		err := response.Body.Close()
		if err != nil {
			slog.Error("fail to close upload session response body", slog.Any("error", err))
		}
	}()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request %s is not successful, get status code: %d, body: %s", url, response.StatusCode, string(body))
	}

	return body, err
}

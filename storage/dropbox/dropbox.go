package dropbox

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/liweiyi88/onedump/filenaming"
)

var (
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

var maxUpload = 150 * MB

type uploadSessionParam struct {
	Close bool `json:"close"`
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
	Path string `yaml:"path"`
	// TODO: currently dropbox disable long lived token and there is no way to complete the oauth flow without browser.
	// Have asked dropbox https://www.dropboxforum.com/t5/Discuss-Dropbox-Developer-API/How-to-get-refresh-token-without-User-interaction/m-p/655155/highlight/true#M3148
	// And see how it goes in the future.
	AccessToken string `yaml:"accesstoken"`
}

func (dropbox *Dropbox) Save(reader io.Reader, gzip bool, unique bool) error {
	offset := 0
	sessionId := ""
	buf := make([]byte, maxUpload)
	client := &http.Client{}

	for {
		n, readErr := reader.Read(buf)
		buf = buf[:n]

		if n > 0 {
			if sessionId == "" {
				sessId, err := dropbox.startUploadSession(client)
				if err != nil {
					return err
				}

				log.Printf("started dropbox upload session %s", sessId)
				sessionId = sessId
			}

			if int64(len(buf)) < maxUpload {
				err := dropbox.uploadSessionFinish(client, buf, offset, sessionId, gzip, unique)
				if err != nil {
					return err
				}

				log.Printf("finish dropbox upload session with offset: %d", offset)

				offset += n
				continue
			}

			err := dropbox.uploadSessionAppend(client, buf, offset, sessionId)
			if err != nil {
				return err
			}

			log.Printf("append dropbox upload session with offset: %d", offset)
			offset += n
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}

			return fmt.Errorf("failed to read from reader :%v", readErr)
		}
	}

	return nil
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
	err = json.Unmarshal(body, sessionResponse)
	if err != nil {
		return "", fmt.Errorf("could not unmarshal upload session response :%v", err)
	}

	return sessionResponse.SessionId, nil
}

func (dropbox *Dropbox) uploadSessionFinish(client *http.Client, data []byte, offset int, sessionId string, gzip bool, unique bool) error {
	bytesReader := bytes.NewReader(data)
	filename := filenaming.EnsureFileName(dropbox.Path, gzip, unique)
	param := uploadSessionFinishParam{
		Commit: Commit{
			Path: filename,
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
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	paramJson, err := json.Marshal(param)
	if err != nil {
		return nil, fmt.Errorf("could not encode param into json %v", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+dropbox.AccessToken)
	req.Header.Set("Dropbox-API-Arg", string(paramJson))

	response, err := client.Do(req)

	defer func() {
		err := response.Body.Close()
		if err != nil {
			log.Printf("failed to close upload session response body: %v", err)
		}
	}()

	if err != nil {
		return nil, fmt.Errorf("failed to send dropbox request %v", err)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	if response.StatusCode >= 300 || response.StatusCode < 200 {
		return nil, fmt.Errorf("request %s is not successful, get status code: %d, body: %s", url, response.StatusCode, string(body))
	}

	return body, err
}

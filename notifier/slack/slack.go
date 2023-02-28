package slack

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/liweiyi88/onedump/jobresult"
)

type Slack struct {
	IncomingWebhook string `yaml:"incomingwebhook"`
}

type SlackMessage struct {
	Blocks []Block `json:"blocks"`
}

type Block struct {
	Type string `json:"type"`
	Text Text   `json:"text"`
}

type Text struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (slack *Slack) Notify(results []*jobresult.JobResult) error {
	if len(results) == 0 {
		return nil
	}

	blocks := make([]Block, 0, len(results)+1)
	title := Text{
		Type: "mrkdwn",
		Text: "*Onedump Results*",
	}

	blocks = append(blocks, Block{
		Type: "section",
		Text: title,
	})

	for _, result := range results {
		text := Text{
			Type: "mrkdwn",
			Text: result.ToSlackText(),
		}

		blocks = append(blocks, Block{
			Type: "section",
			Text: text,
		})
	}

	message := SlackMessage{
		Blocks: blocks,
	}

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal slack message, err: %v", err)
	}

	bytesReader := strings.NewReader(string(data))

	client := &http.Client{}
	res, err := client.Post(slack.IncomingWebhook, "application/json", bytesReader)

	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)

		return fmt.Errorf("slack notification failed: %v", string(body))
	}

	if err != nil {
		return fmt.Errorf("slack notification failed: %v", err)
	}

	return nil
}

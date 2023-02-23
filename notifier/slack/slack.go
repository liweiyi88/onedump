package slack

type Slack struct {
	IncomingWebhook string `yaml:"incomingwebhook"`
}

func (slack *Slack) Notify(message []string) error {
	return nil
}

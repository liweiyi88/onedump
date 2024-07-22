package dumper

import (
	"io"
)

type DBConfig struct {
	DBName   string
	Username string
	Password string
	Host     string
	Port     int
}

func NewDBConfig(dbName, user, password, host string, port int) *DBConfig {
	return &DBConfig{
		DBName:   dbName,
		Username: user,
		Password: password,
		Host:     host,
		Port:     port,
	}
}

// Dump options
type Options map[string]bool

func newOptions(opts ...string) Options {
	options := make(Options, len(opts))
	options.enable(opts...)

	return options
}

func (options Options) isEnabled(option string) bool {
	val, ok := options[option]
	if !ok {
		return false
	}

	return val
}

func (options Options) enable(opts ...string) {
	for _, opt := range opts {
		options[opt] = true
	}
}

type Dumper interface {
	// Dump db content to storage.
	Dump(storage io.Writer) error
}

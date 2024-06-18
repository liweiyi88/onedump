package dumper

import "io"

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

func (options Options) isEnabled(option string) bool {
	ok, value := options[option]
	if !ok {
		return false
	}

	return value
}

type Dumper interface {
	// Dump db content to storage.
	Dump(storage io.Writer) error
}

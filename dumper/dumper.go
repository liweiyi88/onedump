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

type Dumper interface {
	// Dump db content to storage.
	Dump(storage io.Writer) error
}

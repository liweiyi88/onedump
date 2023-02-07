package driver

type Driver interface {
	Close() error
	ExecDumpEnviron() ([]string, error)
	GetExecDumpCommand() (string, []string, error)
	GetSshDumpCommand() (string, error)
}

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

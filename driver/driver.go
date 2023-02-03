package driver

type Driver interface {
	// return dump binary path, dump command and error
	GetDumpCommand() (string, []string, error)
	ExecDumpEnviron() []string
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

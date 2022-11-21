package dbdump

type DbConfig struct {
	DBName   string
	Username string
	Password string
	Host     string
	Port     int
}

func NewDbConfig(dbName, user, password, host string, port int) *DbConfig {
	return &DbConfig{
		DBName:   dbName,
		Username: user,
		Password: password,
		Host:     host,
		Port:     port,
	}
}

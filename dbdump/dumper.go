package dbdump

type DBDump interface {
	Dump(dumpFile string) error
}

type DBDumper struct {
	DBName   string
	Username string
	Password string
	Host     string
	Port     int
}

func NewDBDumper(dbName, user, password, host string, port int) *DBDumper {
	return &DBDumper{
		DBName:   dbName,
		Username: user,
		Password: password,
		Host:     host,
		Port:     port,
	}
}

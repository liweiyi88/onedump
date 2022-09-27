package dbdump

type DBDump interface {
	Dump(dumpFile string) error
}

type DBDumper struct {
	DBName string
	Username string
	Password string
	Host string
	Port int
}

func NewDBDumper() *DBDumper {
	return &DBDumper{
		DBName: "",
		Username: "root",
		Password: "",
		Host: "127.0.0.1",
		Port: 3306,
	}
}
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
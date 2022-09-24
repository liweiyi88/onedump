package dbdump

import (
	"fmt"
	"os"
)

type Mysql struct {
	SkipComments bool
	UseExtendedInserts bool
	UseSingleTransaction bool
	SkipLockTables bool
	DoNotUseColumnStatistics bool
	UseQuick bool
	DefaultCharacterSet string
	SetGtidPurged string
	CreateTables bool
	DBDumper
}

func NewMysqlDumper(dbDumper DBDumper) *Mysql {
	return &Mysql{
		SkipComments: true,
		UseExtendedInserts: true,
		UseSingleTransaction: false,
		SkipLockTables: false,
		DoNotUseColumnStatistics: false,
		UseQuick: false,
		DefaultCharacterSet: "",
		SetGtidPurged: "AUTO",
		CreateTables: true,
		DBDumper: dbDumper,
	}
}

func (mysql *Mysql) createCredentialFile() (string, error) {
	var fileName string

	contents := `[client]
user = %s
password = %s
port = %d
host = %s`

	contents = fmt.Sprintf(contents, mysql.Username, mysql.Password, mysql.Port, mysql.Host)

	file, err := os.CreateTemp("", "mysqldump-")
	if err != nil {
		return fileName, fmt.Errorf("failed to create temp folder: %w", err)
	}

	defer file.Close()

	_, err = file.WriteString(contents)
	if err != nil {
		return fileName, fmt.Errorf("failed to write credentials to temp file: %w", err)
	}

	return file.Name(), nil
}

func (mysql *Mysql) Dump(dumpFile string) error {
	return nil
}
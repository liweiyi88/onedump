package dumper

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"log/slog"

	"github.com/go-sql-driver/mysql"
	"github.com/liweiyi88/onedump/config"
)

type MysqlNativeDump struct {
	options []string
	viaSsh  bool
	sshHost string
	sshUser string
	sshKey  string
	db      *sql.DB
}

func NewMysqlNativeDump(job *config.Job) (*MysqlNativeDump, error) {
	dsn := job.DBDsn
	options := job.DumpOptions

	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	pingErr := db.Ping()
	if pingErr != nil {
		return nil, pingErr
	}

	slog.Info("database connected.")

	commandOptions := []string{"--skip-comments", "--extended-insert"}

	if len(options) > 0 {
		commandOptions = options
	}

	return &MysqlNativeDump{
		options: commandOptions,
		viaSsh:  job.ViaSsh(),
		sshHost: job.SshHost,
		sshUser: job.SshUser,
		sshKey:  job.SshKey,
		db:      db,
	}, nil
}

func (mysql *MysqlNativeDump) getTables() ([]string, error) {
	rows, err := mysql.db.Query("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to query all tables, err: %v", err)
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			slog.Error("failed to close rows", slog.Any("error", err))
		}
	}()

	var tables []string

	for rows.Next() {
		var table string

		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan tables, err: %v", err)
		}

		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to get all tables, err: %v", err)
	}

	return tables, nil
}

func (mysql *MysqlNativeDump) Dump(storage io.Writer) error {
	defer func() {
		err := mysql.db.Close()
		if err != nil {
			slog.Error("failed to close db", slog.Any("error", err))

		}
	}()

	tables, err := mysql.getTables()

	if err != nil {
		return err
	}

	fmt.Print(tables)

	return nil
}

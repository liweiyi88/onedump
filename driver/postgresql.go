package driver

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/jackc/pgx/v5"
)

type PostgreSqlDriver struct {
	PgDumpBinaryPath string
	Options          []string
	ViaSsh           bool
	*DBConfig
}

// Dsn example: postgres://username:password@localhost:5432/database_name"
func NewPostgreSqlDriver(dsn string, options []string, viaSsh bool) (*PostgreSqlDriver, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	return &PostgreSqlDriver{
		PgDumpBinaryPath: "pg_dump",
		Options:          options,
		ViaSsh:           viaSsh,
		DBConfig:         NewDBConfig(config.Database, config.User, config.Password, config.Host, int(config.Port)),
	}, nil
}

func (psql *PostgreSqlDriver) getDumpCommandArgs() []string {
	args := []string{}

	args = append(args, "--host="+psql.Host)
	args = append(args, "--username="+psql.Username)
	args = append(args, "--dbname="+psql.DBName)
	args = append(args, psql.Options...)

	return args
}

func (psql *PostgreSqlDriver) GetDumpCommand() (string, []string, error) {
	pgDumpPath, err := exec.LookPath(psql.PgDumpBinaryPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to find pg_dump executable %s %w", psql.PgDumpBinaryPath, err)
	}

	return pgDumpPath, psql.getDumpCommandArgs(), nil
}

func (psql *PostgreSqlDriver) ExecDumpEnviron() []string {
	env := []string{fmt.Sprintf("PGPASSWORD=%s", psql.Password)}
	return env
}

func (psql *PostgreSqlDriver) GetSshDumpCommand() (string, error) {
	return fmt.Sprintf("PGPASSWORD=%s pg_dump %s", psql.Password, strings.Join(psql.getDumpCommandArgs(), " ")), nil
}

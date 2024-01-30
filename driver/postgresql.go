package driver

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v5"

	"github.com/liweiyi88/onedump/fileutil"
)

type PostgreSqlDriver struct {
	credentialFiles  []string
	PgDumpBinaryPath string
	Options          []string
	ViaSsh           bool
	*DBConfig
}

// Create the postgressql dump driver.
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
	args = append(args, "--port="+strconv.Itoa(psql.Port))
	args = append(args, "--username="+psql.Username)
	args = append(args, "--dbname="+psql.DBName)
	args = append(args, psql.Options...)

	return args
}

// Get the exec dump command.
func (psql *PostgreSqlDriver) GetExecDumpCommand() (string, []string, error) {
	pgDumpPath, err := exec.LookPath(psql.PgDumpBinaryPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to find pg_dump executable %s %w", psql.PgDumpBinaryPath, err)
	}

	return pgDumpPath, psql.getDumpCommandArgs(), nil
}

// Get the required environment variables for running exec dump.
func (psql *PostgreSqlDriver) ExecDumpEnviron() ([]string, error) {
	pgpassFileName, err := psql.createCredentialFile()
	if err != nil {
		return nil, err
	}

	env := []string{fmt.Sprintf("PGPASSFILE=%s", pgpassFileName)}
	return env, nil
}

// Get the ssh dump command.
func (psql *PostgreSqlDriver) GetSshDumpCommand() (string, error) {
	return fmt.Sprintf("PGPASSWORD=%s pg_dump %s", psql.Password, strings.Join(psql.getDumpCommandArgs(), " ")), nil
}

// Cleanup the credentials file.
func (psql *PostgreSqlDriver) Close() error {
	var err error
	if len(psql.credentialFiles) > 0 {
		for _, filename := range psql.credentialFiles {
			if e := os.Remove(filename); e != nil {
				err = multierror.Append(err, e)
			}
		}

		psql.credentialFiles = nil
	}

	return err
}

// Store the username password in a temp file, and use it with the pg_dump command.
// It avoids to expoes credentials when you run the pg_dump command as user can view the whole command via ps aux.
func (psql *PostgreSqlDriver) createCredentialFile() (string, error) {
	file, err := os.Create(fileutil.WorkDir() + "/.pgpass" + fileutil.GenerateRandomName(4))
	if err != nil {
		return "", fmt.Errorf("could not create .pgpass file: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("could not close file: %s, err: %v", file.Name(), err)
		}
	}()

	contents := fmt.Sprintf("%s:%d:%s:%s:%s", psql.Host, psql.Port, psql.DBName, psql.Username, psql.Password)
	_, err = file.WriteString(contents)
	if err != nil {
		return file.Name(), fmt.Errorf("failed to write credentials to .pgpass file: %w", err)
	}

	if err = os.Chmod(file.Name(), 0600); err != nil {
		log.Printf("could not change file permissoin, file: %s, error: %v", file.Name(), err)
	}

	psql.credentialFiles = append(psql.credentialFiles, file.Name())

	return file.Name(), nil
}

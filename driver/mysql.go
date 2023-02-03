package driver

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

const CredentialFilePrefix = "mysqldumpcred-"

type MysqlDriver struct {
	MysqlDumpBinaryPath string
	Options             []string
	ViaSsh              bool
	*DBConfig
}

func NewMysqlDriver(dsn string, options []string, viaSsh bool) (*MysqlDriver, error) {
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	host, port, err := net.SplitHostPort(config.Addr)
	if err != nil {
		return nil, err
	}

	dbPort, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}

	commandOptions := []string{"--skip-comments", "--extended-insert"}

	if len(options) > 0 {
		commandOptions = options
	}

	return &MysqlDriver{
		MysqlDumpBinaryPath: "mysqldump",
		Options:             commandOptions,
		ViaSsh:              viaSsh,
		DBConfig:            NewDBConfig(config.DBName, config.User, config.Passwd, host, dbPort),
	}, nil
}

func (mysql *MysqlDriver) GetDumpCommand() (string, []string, error) {
	args, err := mysql.getDumpCommandArgs()

	if err != nil {
		return "", nil, fmt.Errorf("failed to get dump command args %w", err)
	}

	mysqldumpBinaryPath, err := exec.LookPath(mysql.MysqlDumpBinaryPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to find mysqldump executable %s %w", mysql.MysqlDumpBinaryPath, err)
	}

	return mysqldumpBinaryPath, args, nil
}

// Get dump command used by ssh dumper.
func (mysql *MysqlDriver) GetSshDumpCommand() (string, error) {
	args, err := mysql.getDumpCommandArgs()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("mysqldump %s", strings.Join(args, " ")), nil
}

// Store the username password in a temp file, and use it with the mysqldump command.
// It avoids to expoes credentials when you run the mysqldump command as user can view the whole command via ps aux.
// Inspired by https://github.com/spatie/db-dumper
func (mysql *MysqlDriver) getDumpCommandArgs() ([]string, error) {
	args := []string{}

	if !mysql.ViaSsh {
		credentialsFileName, err := mysql.createCredentialFile()
		if err != nil {
			return nil, err
		}
		args = append(args, "--defaults-extra-file="+credentialsFileName+"")
	} else {
		args = append(args, "-u "+mysql.Username+" -p"+mysql.Password)
	}

	args = append(args, mysql.Options...)
	args = append(args, mysql.DBName)

	return args, nil
}

func (mysql *MysqlDriver) createCredentialFile() (string, error) {
	var fileName string

	contents := `[client]
user = %s
password = %s
port = %d
host = %s`

	contents = fmt.Sprintf(contents, mysql.Username, mysql.Password, mysql.Port, mysql.Host)

	file, err := os.CreateTemp("", CredentialFilePrefix)
	if err != nil {
		return fileName, fmt.Errorf("failed to create temp folder: %w", err)
	}

	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close temp file for storing mysql credentials: %v", err)
		}
	}()

	_, err = file.WriteString(contents)
	if err != nil {
		return fileName, fmt.Errorf("failed to write credentials to temp file: %w", err)
	}

	return file.Name(), nil
}

func (mysql *MysqlDriver) ExecDumpEnviron() []string {
	return nil
}

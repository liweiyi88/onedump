package dump

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

const CredentialFilePrefix = "mysqldumpcred-"

type Mysql struct {
	MysqlDumpBinaryPath string
	Options             []string
	ViaSsh              bool
	*DBConfig
}

func NewMysqlDumper(dsn string, options []string, viaSsh bool) (*Mysql, error) {
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

	return &Mysql{
		MysqlDumpBinaryPath: "mysqldump",
		Options:             commandOptions,
		ViaSsh:              viaSsh,
		DBConfig:            NewDBConfig(config.DBName, config.User, config.Passwd, host, dbPort),
	}, nil
}

// Get dump command used by ssh dumper.
func (mysql *Mysql) GetSshDumpCommand() (string, error) {
	args, err := mysql.getDumpCommandArgs()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("mysqldump %s", strings.Join(args, " ")), nil
}

// Store the username password in a temp file, and use it with the mysqldump command.
// It avoids to expoes credentials when you run the mysqldump command as user can view the whole command via ps aux.
// Inspired by https://github.com/spatie/db-dumper
func (mysql *Mysql) getDumpCommandArgs() ([]string, error) {

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

func (mysql *Mysql) createCredentialFile() (string, error) {
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

	defer file.Close()

	_, err = file.WriteString(contents)
	if err != nil {
		return fileName, fmt.Errorf("failed to write credentials to temp file: %w", err)
	}

	return file.Name(), nil
}

func (mysql *Mysql) Dump(dumpFile string, shouldGzip bool) error {
	args, err := mysql.getDumpCommandArgs()

	if err != nil {
		return fmt.Errorf("failed to get dump command args %w", err)
	}

	// check and get the binary path.
	mysqldumpBinaryPath, err := exec.LookPath(mysql.MysqlDumpBinaryPath)
	if err != nil {
		return fmt.Errorf("failed to find mysqldump executable %s %w", mysql.MysqlDumpBinaryPath, err)
	}

	cmd := exec.Command(mysqldumpBinaryPath, args...)

	copyDump, persistDump, err := dump(dumpFile, shouldGzip)
	if err != nil {
		return err
	}

	// by assigning os.Stderr to cmd.Stderr, if it fails to run the command, os.Stderr will also output the error details.
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start command error: %v", err)
	}

	if err != nil {
		return fmt.Errorf("failed to get cmd stdout pipe %w", err)
	}

	err = copyDump(stdout)
	if err != nil {
		return fmt.Errorf("failed to copy content from cmd stdout to io writer. %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("cmd command error: %v", err)
	}

	err = persistDump()
	if err != nil {
		return fmt.Errorf("faile to persist the dump file %w", err)
	}

	return nil
}

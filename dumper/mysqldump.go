package dumper

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"slices"

	"github.com/go-sql-driver/mysql"
	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/dumper/runner"
	"github.com/liweiyi88/onedump/fileutil"
)

type MysqlDump struct {
	credentialFiles []string
	path            string
	options         []string
	viaSsh          bool
	sshHost         string
	sshUser         string
	sshKey          string
	*DBConfig
}

func NewMysqlDump(job *config.Job) (*MysqlDump, error) {
	dsn := job.DBDsn
	options := job.DumpOptions

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

	return &MysqlDump{
		path:     "mysqldump",
		options:  commandOptions,
		viaSsh:   job.ViaSsh(),
		sshHost:  job.SshHost,
		sshUser:  job.SshUser,
		sshKey:   job.SshKey,
		DBConfig: NewDBConfig(config.DBName, config.User, config.Passwd, host, dbPort),
	}, nil
}

// Get the exec dump command.
func (mysql *MysqlDump) getExecDumpCommand() (string, []string, error) {
	args, err := mysql.getDumpCommandArgs()

	if err != nil {
		return "", nil, fmt.Errorf("failed to get dump command args %w", err)
	}

	mysqldumpBinaryPath, err := exec.LookPath(mysql.path)
	if err != nil {
		return "", nil, fmt.Errorf("failed to find mysqldump executable %s %w", mysql.path, err)
	}

	return mysqldumpBinaryPath, args, nil
}

// Get dump command used by ssh dumper.
func (mysql *MysqlDump) getSshDumpCommand() (string, error) {
	args, err := mysql.getDumpCommandArgs()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("mysqldump %s", strings.Join(args, " ")), nil
}

func (mysql *MysqlDump) getDumpCommandArgs() ([]string, error) {
	args := []string{}

	if !mysql.viaSsh {
		credentialsFileName, err := mysql.createCredentialFile()
		if err != nil {
			return nil, err
		}
		args = append(args, "--defaults-extra-file="+credentialsFileName+"")
	} else {
		args = append(args, "--host "+mysql.Host)
		args = append(args, "--port "+strconv.Itoa(mysql.Port))
		args = append(args, "-u "+mysql.Username+" -p"+mysql.Password)
	}

	args = append(args, mysql.options...)

	if !slices.Contains(mysql.options, "--all-databases") {
		args = append(args, mysql.DBName)
	}

	return args, nil
}

// Store the username password in a temp file, and use it with the mysqldump command.
// It avoids to expoes credentials when you run the mysqldump command as user can view the whole command via ps aux.
func (mysql *MysqlDump) createCredentialFile() (string, error) {
	contents := `[client]
user = %s
password = %s
port = %d
host = %s`

	contents = fmt.Sprintf(contents, mysql.Username, mysql.Password, mysql.Port, mysql.Host)

	file, err := os.Create(fileutil.WorkDir() + "/.mysqlpass" + fileutil.GenerateRandomName(4))
	if err != nil {
		return file.Name(), fmt.Errorf("failed to create temp mysql credentials file: %w", err)
	}

	defer func() {
		err := file.Close()
		if err != nil {
			slog.Error("fail to close temp file for storing mysql credentials", slog.Any("error", err))
		}
	}()

	_, err = file.WriteString(contents)
	if err != nil {
		return file.Name(), fmt.Errorf("failed to write credentials to temp file: %w", err)
	}

	mysql.credentialFiles = append(mysql.credentialFiles, file.Name())

	return file.Name(), nil
}

// Cleanup the credentials file.
func (mysql *MysqlDump) close() error {
	var errs error
	if len(mysql.credentialFiles) > 0 {
		for _, filename := range mysql.credentialFiles {
			if e := os.Remove(filename); e != nil {
				errs = errors.Join(errs, e)
			}
		}

		mysql.credentialFiles = nil
	}

	return errs
}

func (mysql *MysqlDump) Dump(storage io.Writer) error {
	defer func() {
		if err := mysql.close(); err != nil {
			slog.Error("could not mysqldump credential files db driver", slog.Any("error", err))
		}
	}()

	if mysql.viaSsh {
		command, err := mysql.getSshDumpCommand()
		if err != nil {
			return err
		}

		return runner.NewSshRunner(mysql.sshHost, mysql.sshKey, mysql.sshUser, command).Run(storage)
	}

	command, args, err := mysql.getExecDumpCommand()
	if err != nil {
		return err
	}

	return runner.NewExecRunner(command, args, nil).Run(storage)
}

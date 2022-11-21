package dbdump

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const CredentialFilePrefix = "mysqldumpcred-"

type Mysql struct {
	MysqlDumpBinaryPath string
	Options             []string
	ViaSsh              bool
	*DbConfig
}

func NewMysqlDumper(dbName, user, password, host string, port int, options []string, viaSsh bool) *Mysql {
	commandOptions := []string{"--skip-comments", "--extended-insert"}

	if len(options) > 0 {
		commandOptions = options
	}

	return &Mysql{
		MysqlDumpBinaryPath: "mysqldump",
		Options:             commandOptions,
		ViaSsh:              viaSsh,
		DbConfig:            NewDbConfig(dbName, user, password, host, port),
	}
}

// Get dump command used by ssh dumper.
func (mysql *Mysql) GetSshDumpCommand(dumpFile string) (string, error) {
	args, err := mysql.getDumpCommandArgs()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("mysqldump %s > %s", strings.Join(args, " "), dumpFile), nil
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

func (mysql *Mysql) Dump(dumpFile string) error {
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

	dumpOutFile, err := os.Create(dumpFile)
	if err != nil {
		return fmt.Errorf("failed to create the dump file %w", err)
	}
	defer dumpOutFile.Close()

	// io copy the content from the stdout to the dump file.
	cmd.Stdout = dumpOutFile

	// by assigning os.Stderr to cmd.Stderr, if it fails to run the command, os.Stderr will also output the error details.
	cmd.Stderr = os.Stderr

	err = cmd.Run()

	if err != nil {
		return fmt.Errorf("failed to run dump command %w", err)
	}

	fmt.Println("db dump succeed, dump file: ", dumpOutFile.Name())

	return nil
}

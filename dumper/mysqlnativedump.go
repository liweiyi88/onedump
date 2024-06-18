package dumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/liweiyi88/onedump/config"
)

const (
	skipAddDropTable = "--skip-add-drop-table"
)

type MysqlNativeDump struct {
	options Options
	viaSsh  bool
	sshHost string
	sshUser string
	sshKey  string
	*DBConfig
	db *sql.DB
}

func NewMysqlNativeDump(job *config.Job) (*MysqlNativeDump, error) {
	dsn := job.DBDsn

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

	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	pingErr := db.Ping()
	if pingErr != nil {
		return nil, pingErr
	}

	slog.Info("database connected.")

	options := make(map[string]bool)
	options[skipAddDropTable] = false

	for _, opt := range job.DumpOptions {
		ok := options[opt]
		if ok {
			options[opt] = true
		}
	}

	return &MysqlNativeDump{
		options:  options,
		viaSsh:   job.ViaSsh(),
		sshHost:  job.SshHost,
		sshUser:  job.SshUser,
		sshKey:   job.SshKey,
		DBConfig: NewDBConfig(config.DBName, config.User, config.Passwd, host, dbPort),
		db:       db,
	}, nil
}

func (mysql *MysqlNativeDump) getCharacterSet() (string, error) {
	var variableName string
	var characterSet string

	row := mysql.db.QueryRow("SHOW VARIABLES LIKE 'character_set_database'")
	err := row.Scan(&variableName, &characterSet)

	if err != nil {
		return "", err
	}

	return characterSet, nil
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

func (mysql *MysqlNativeDump) writeTableContent(buf *bufio.Writer, table string) error {
	results, err := mysql.db.Query(fmt.Sprintf("SELECT * FROM `%s`;", table))

	if err != nil {
		return fmt.Errorf("failed to query table: %s, err: %v", table, err)
	}

	defer func() {
		err := results.Close()
		if err != nil {
			slog.Error("failed to close rows", slog.Any("error", err))
		}
	}()

	columns, err := results.Columns()
	if err != nil {
		return err
	}

	columnTypes, err := results.ColumnTypes()
	if err != nil {
		return err
	}

	var rows [][]any
	for results.Next() {
		row := make([]any, len(columns))
		dest := make([]any, len(columns))

		for i := range row {
			dest[i] = &row[i]
		}

		err = results.Scan(dest...)
		if err != nil {
			return err
		}

		rows = append(rows, row)
	}

	for _, row := range rows {
		for i, col := range row {
			// check https://go.dev/wiki/SQLInterface
			fmt.Println(col, columnTypes[i].ScanType())
			// write insert statement.
		}
	}

	return nil
}

func (mysql *MysqlNativeDump) writeTableStructure(buf *bufio.Writer, table string) error {
	if !mysql.options.isEnabled(skipAddDropTable) {
		buf.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table))
	}

	var name string
	var createTable string

	row := mysql.db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	err := row.Scan(&name, &createTable)

	if err != nil {
		return fmt.Errorf("faile to scan create table structure for table: %s, error: %v", table, err)
	}

	_, err = buf.WriteString(createTable)
	buf.WriteString("\n\n")
	if err != nil {
		return fmt.Errorf("faile to write create table structure for table: %s, error: %v", table, err)
	}

	return nil
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

	charSet, err := mysql.getCharacterSet()
	if err != nil {
		return err
	}

	buf := bufio.NewWriter(storage)
	defer buf.Flush()

	buf.WriteString("-- -------------------------------------------------------------\n")
	buf.WriteString("-- Onedump\n")
	buf.WriteString("--\n")
	buf.WriteString("-- https://github.com/liweiyi88/onedump\n")
	buf.WriteString("--\n")
	buf.WriteString("-- Database: " + mysql.DBName + "\n")
	buf.WriteString("\n\n")
	buf.WriteString("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n")
	buf.WriteString("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n")
	buf.WriteString("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n")
	buf.WriteString("/*!40101 SET NAMES " + charSet + " */;\n")
	buf.WriteString("/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;\n")
	buf.WriteString("/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n")
	buf.WriteString("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n")
	buf.WriteString("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;\n")
	buf.WriteString("\n\n")

	mysql.db.Stats()
	for _, table := range tables {
		err := mysql.writeTableStructure(buf, table)

		if err != nil {
			return err
		}
	}

	for _, table := range tables {
		err := mysql.writeTableContent(buf, table)
		return err
	}

	// then dump content.

	buf.WriteString("/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;\n")
	buf.WriteString("/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n")
	buf.WriteString("/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;\n")
	buf.WriteString("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n")
	buf.WriteString("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n")
	buf.WriteString("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n")
	buf.WriteString("/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;")

	return nil
}

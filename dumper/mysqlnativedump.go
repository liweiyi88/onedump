package dumper

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

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
		return fmt.Errorf("could not get columns: %v", err)
	}

	columnTypes, err := results.ColumnTypes()
	if err != nil {
		return fmt.Errorf("could not get column types: %v", err)
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
			return fmt.Errorf("failed to scan row to dest, %v", err)
		}

		rows = append(rows, row)
	}

	for _, row := range rows {
		var sb strings.Builder
		sb.WriteString("INSERT INTO `" + table + "` VALUES (")

		for i, value := range row {
			if value == nil {
				sb.WriteString("NULL")
			} else {
				typeName := columnTypes[i].DatabaseTypeName()

				switch typeName {
				case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
					if v, ok := value.([]byte); ok {
						sb.WriteString(string(v))
					} else {
						sb.WriteString(fmt.Sprintf("%d", value))
					}
				case "FLOAT", "DOUBLE":
					if v, ok := value.([]byte); ok {
						sb.WriteString(string(v))
					} else {
						sb.WriteString(fmt.Sprintf("%f", value))
					}
				case "DECIMAL", "DEC":
					sb.WriteString(fmt.Sprintf("%s", value))
				case "DATE":
					v, ok := value.(time.Time)
					if !ok {
						return fmt.Errorf("could not parse DATE type, error: %v", err)
					}
					sb.WriteString(fmt.Sprintf("'%s'", v.Format("2006-01-02")))
				case "DATETIME":
					v, ok := value.([]byte)
					if !ok {
						return fmt.Errorf("could not parse DATETIME type, error: %v", err)
					}

					sb.WriteString(fmt.Sprintf("'%s'", string(v)))
				case "TIMESTAMP":
					v, ok := value.([]byte)
					if !ok {
						return fmt.Errorf("could not parse TIMESTAMP type, error: %v", err)
					}
					sb.WriteString(fmt.Sprintf("'%s'", string(v)))
				case "TIME":
					v, ok := value.([]byte)
					if !ok {
						return fmt.Errorf("could not parse time type, error: %v", err)
					}
					sb.WriteString(fmt.Sprintf("'%s'", string(v)))
				case "YEAR":
					v, ok := value.(int64)
					if !ok {
						return fmt.Errorf("cloud not parse YEAR type, error: %v", err)
					}
					sb.WriteString(fmt.Sprintf("'%d'", v))
				case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
					sb.WriteString(fmt.Sprintf("'%s'", strings.Replace(fmt.Sprintf("%s", value), "'", "''", -1)))
				case "BINARY":
					v, ok := value.([]uint8)
					if !ok {
						return fmt.Errorf("cloud not parse BINARY type, error: %v", err)
					}

					v = bytes.TrimRight(v, "\x00") // skip trailing null value
					sb.WriteString(fmt.Sprintf("'%s'", v))
				case "VARBINARY":
					sb.WriteString(fmt.Sprintf("'%s'", value))
				case "BIT":
					v, ok := value.([]uint8)
					if !ok {
						return fmt.Errorf("cloud not parse BIT type, error: %v", err)
					}

					if len(v) > 1 {
						return fmt.Errorf("failed to parse BIT type, expected length 1, but got %d", len(v))
					}

					sb.WriteString(fmt.Sprintf("b'%d'", v[0]))
				case "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "ENUM", "SET":
					sb.WriteString(fmt.Sprintf("'%s'", value))
				case "BOOL", "BOOLEAN":
					if value.(bool) {
						sb.WriteString("true")
					} else {
						sb.WriteString("false")
					}
				case "JSON":
					v, ok := value.([]uint8)
					if !ok {
						return fmt.Errorf("cloud not parse JSON type, expect []unint8 but got %T", value)
					}

					json := string(v)

					json = strings.ReplaceAll(json, `"`, `\"`)
					sb.WriteString(fmt.Sprintf("'%s'", json))
				default:
					return fmt.Errorf("unsupported type database type: %s", typeName)
				}
			}

			if i < len(row)-1 {
				sb.WriteString(",")
			}
		}
		sb.WriteString(");\n")

		_, err := buf.WriteString(sb.String())
		if err != nil {
			return fmt.Errorf("failed to write insert statement: %v", err)
		}
	}

	return nil
}

func (mysql *MysqlNativeDump) writeHeader(buf *bufio.Writer) error {
	charSet, err := mysql.getCharacterSet()
	if err != nil {
		return err
	}

	var sb strings.Builder

	sb.WriteString("-- -------------------------------------------------------------\n")
	sb.WriteString("-- Onedump\n")
	sb.WriteString("--\n")
	sb.WriteString("-- https://github.com/liweiyi88/onedump\n")
	sb.WriteString("--\n")
	sb.WriteString("-- Database: " + mysql.DBName + "\n")
	sb.WriteString("\n\n")
	sb.WriteString("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n")
	sb.WriteString("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n")
	sb.WriteString("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n")
	sb.WriteString("/*!40101 SET NAMES " + charSet + " */;\n")
	sb.WriteString("/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;\n")
	sb.WriteString("/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n")
	sb.WriteString("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n")
	sb.WriteString("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;\n")
	sb.WriteString("\n\n")

	_, err = buf.WriteString(sb.String())
	return err
}

func (mysql *MysqlNativeDump) writeFooter(buf *bufio.Writer) error {
	var sb strings.Builder
	sb.WriteString("\n\n")
	sb.WriteString("/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;\n")
	sb.WriteString("/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n")
	sb.WriteString("/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;\n")
	sb.WriteString("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n")
	sb.WriteString("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n")
	sb.WriteString("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n")
	sb.WriteString("/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;")

	_, err := buf.WriteString(sb.String())
	return err
}

func (mysql *MysqlNativeDump) writeTableStructure(buf *bufio.Writer, table string) error {
	var sb strings.Builder

	if !mysql.options.isEnabled(skipAddDropTable) {
		sb.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table))
	}

	var name string
	var createTable string

	row := mysql.db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	err := row.Scan(&name, &createTable)

	if err != nil {
		return fmt.Errorf("faile to scan create table structure for table: %s, error: %v", table, err)
	}

	sb.WriteString(createTable + ";")
	sb.WriteString("\n\n")

	_, err = buf.WriteString(sb.String())
	return err
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

	buf := bufio.NewWriter(storage)
	defer buf.Flush()

	err = mysql.writeHeader(buf)
	if err != nil {
		return fmt.Errorf("failed to write dump header, error: %v", err)
	}

	for _, table := range tables {
		err := mysql.writeTableStructure(buf, table)
		if err != nil {
			return fmt.Errorf("failed to write table structure, table: %s, error: %v", table, err)
		}
	}

	for _, table := range tables {
		err := mysql.writeTableContent(buf, table)

		if err != nil {
			return fmt.Errorf("failed to write table content, table: %s, error: %v", table, err)
		}
	}

	err = mysql.writeFooter(buf)
	if err != nil {
		return fmt.Errorf("failed to write dump footer, error: %v", err)
	}

	return nil
}

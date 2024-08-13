package dumper

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/dumper/dialer"
)

const (
	skipAddDropTable = "--skip-add-drop-table"
	skipAddLocks     = "--skip-add-locks"
)

type MysqlNativeDump struct {
	options  Options
	viaSsh   bool
	sshHost  string
	sshUser  string
	sshKey   string
	DBConfig *mysql.Config
	db       *sql.DB
}

func NewMysqlNativeDump(job *config.Job) (*MysqlNativeDump, error) {
	dsn := job.DBDsn

	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return &MysqlNativeDump{
		options:  newOptions(job.DumpOptions...),
		viaSsh:   job.ViaSsh(),
		sshHost:  job.SshHost,
		sshUser:  job.SshUser,
		sshKey:   job.SshKey,
		DBConfig: config,
	}, nil
}

func (m *MysqlNativeDump) getCharacterSet() (string, error) {
	var variableName string
	var characterSet string

	row := m.db.QueryRow("SHOW VARIABLES LIKE 'character_set_database'")
	err := row.Scan(&variableName, &characterSet)

	if err != nil {
		return "", err
	}

	return characterSet, nil
}

func (m *MysqlNativeDump) getTables() ([]string, error) {
	rows, err := m.db.Query("SHOW TABLES")
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

func (m *MysqlNativeDump) writeTableContent(buf *bufio.Writer, table string) error {
	results, err := m.db.Query(fmt.Sprintf("SELECT * FROM `%s`;", table))

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

	var sb strings.Builder

	if len(rows) > 0 {
		if !m.options.isEnabled(skipAddLocks) {
			sb.WriteString("LOCK TABLES `" + table + "` WRITE;\n")
		}

		sb.WriteString("/*!40000 ALTER TABLE `" + table + "` DISABLE KEYS */;\n")
		sb.WriteString("INSERT INTO `" + table + "` (")
		for i, col := range columns {
			if i < len(columns)-1 {
				sb.WriteString("`" + col + "`, ")
			} else {
				sb.WriteString("`" + col + "`)")
			}
		}

		sb.WriteString(" VALUES ")
	}

	for rowIndex, row := range rows {
		sb.WriteString("(")

		for colIndex, value := range row {
			if value == nil {
				sb.WriteString("NULL")
			} else {
				typeName := columnTypes[colIndex].DatabaseTypeName()

				switch typeName {
				case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "UNSIGNED INT":
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
					v, ok := value.([]uint8)

					if !ok {
						return fmt.Errorf("could not parse DATE type, expect []uint8, got %T", value)
					}

					sb.WriteString(fmt.Sprintf("'%s'", v))
				case "DATETIME":
					v, ok := value.([]byte)
					if !ok {
						return fmt.Errorf("could not parse DATETIME type, expect []byte, got %T", value)
					}

					sb.WriteString(fmt.Sprintf("'%s'", string(v)))
				case "TIMESTAMP":
					v, ok := value.([]byte)
					if !ok {
						return fmt.Errorf("could not parse TIMESTAMP type, expect []byte, got %T", value)
					}
					sb.WriteString(fmt.Sprintf("'%s'", string(v)))
				case "TIME":
					v, ok := value.([]byte)
					if !ok {
						return fmt.Errorf("could not parse TIME type, expect []byte, got %T", value)
					}
					sb.WriteString(fmt.Sprintf("'%s'", string(v)))
				case "YEAR":
					v, ok := value.(int64)
					if !ok {
						return fmt.Errorf("could not parse YEAR type, expect int64, got %T", value)
					}
					sb.WriteString(fmt.Sprintf("'%d'", v))
				case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
					sb.WriteString(fmt.Sprintf("'%s'", strings.Replace(fmt.Sprintf("%s", value), "'", "''", -1)))
				case "BINARY":
					v, ok := value.([]uint8)
					if !ok {
						return fmt.Errorf("could not parse BINARY type, expect []uint8, got %T", value)
					}

					v = bytes.TrimRight(v, "\x00") // skip trailing null value
					sb.WriteString(fmt.Sprintf("'%s'", v))
				case "VARBINARY":
					sb.WriteString(fmt.Sprintf("'%s'", value))
				case "BIT":
					v, ok := value.([]uint8)
					if !ok {
						return fmt.Errorf("cloud not parse BIT type, expect []uint8, got %T", value)
					}

					if len(v) > 1 {
						return fmt.Errorf("failed to parse BIT type, expected length 1, got %d", len(v))
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
						return fmt.Errorf("cloud not parse JSON type, expect []unint8, got %T", value)
					}

					json := string(v)

					json = strings.ReplaceAll(json, `"`, `\"`)
					sb.WriteString(fmt.Sprintf("'%s'", json))
				default:
					return fmt.Errorf("unsupported database type: %s", typeName)
				}
			}

			if colIndex < len(row)-1 {
				sb.WriteString(",")
			}
		}

		sb.WriteString(")")

		if rowIndex < len(rows)-1 {
			sb.WriteString(",")
		} else {
			sb.WriteString(";")
		}
	}

	sb.WriteString("\n/*!40000 ALTER TABLE `" + table + "` ENABLE KEYS */;\n")
	if !m.options.isEnabled(skipAddLocks) {
		sb.WriteString("UNLOCK TABLES;")
	}

	sb.WriteString("\n\n")

	_, err = buf.WriteString(sb.String())
	if err != nil {
		return fmt.Errorf("failed to write insert statement: %s, error: %v", sb.String(), err)
	}

	return nil
}

func (m *MysqlNativeDump) writeHeader(buf *bufio.Writer) error {
	charSet, err := m.getCharacterSet()
	if err != nil {
		return err
	}

	var sb strings.Builder

	sb.WriteString("-- -------------------------------------------------------------\n")
	sb.WriteString("-- Onedump\n")
	sb.WriteString("--\n")
	sb.WriteString("-- https://github.com/liweiyi88/onedump\n")
	sb.WriteString("--\n")
	sb.WriteString("-- Database: " + m.DBConfig.DBName + "\n")
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

func (m *MysqlNativeDump) writeFooter(buf *bufio.Writer) error {
	var sb strings.Builder
	sb.WriteString("\n\n")
	sb.WriteString("/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;\n")
	sb.WriteString("/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n")
	sb.WriteString("/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;\n")
	sb.WriteString("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n")
	sb.WriteString("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n")
	sb.WriteString("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n")
	sb.WriteString("/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;")
	sb.WriteString("\n\n")

	_, err := buf.WriteString(sb.String())
	return err
}

func (m *MysqlNativeDump) writeTableStructure(buf *bufio.Writer, table string) error {
	var sb strings.Builder

	if !m.options.isEnabled(skipAddDropTable) {
		sb.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table))
	}

	var name string
	var createTable string

	row := m.db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	err := row.Scan(&name, &createTable)

	if err != nil {
		return fmt.Errorf("faile to scan create table structure for table: %s, error: %v", table, err)
	}

	sb.WriteString(createTable + ";")
	sb.WriteString("\n\n")

	_, err = buf.WriteString(sb.String())
	return err
}

func (m *MysqlNativeDump) Dump(storage io.Writer) error {
	if m.viaSsh {
		sshClient, err := dialer.NewSsh(m.sshHost, m.sshKey, m.sshUser).CreateSshClient()

		if err != nil {
			return fmt.Errorf("failed to create sshClient: %v", err)
		}

		defer func() {
			err := sshClient.Close()

			if err != nil {
				slog.Error("failed to close SSH connection", slog.Any("error", err))
			}

			slog.Debug("SSH connection closed")
		}()

		mysql.RegisterDialContext("tcp", func(ctx context.Context, addr string) (net.Conn, error) {
			if err != nil {
				return nil, fmt.Errorf("failed to create ssh client, error: %v", err)
			}

			return sshClient.Dial("tcp", addr)
		})
	}

	db, err := sql.Open("mysql", m.DBConfig.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	pingErr := db.Ping()

	if pingErr != nil {
		return pingErr
	}

	m.db = db
	slog.Debug("database connected.")

	defer func() {
		err := m.db.Close()
		if err != nil {
			slog.Error("failed to close db", slog.Any("error", err))
		}
	}()

	tables, err := m.getTables()
	if err != nil {
		return err
	}

	buf := bufio.NewWriter(storage)
	defer buf.Flush()

	err = m.writeHeader(buf)
	if err != nil {
		return fmt.Errorf("failed to write dump header, error: %v", err)
	}

	for _, table := range tables {
		err := m.writeTableStructure(buf, table)
		if err != nil {
			return fmt.Errorf("failed to write table structure, table: %s, error: %v", table, err)
		}
	}

	for _, table := range tables {
		err := m.writeTableContent(buf, table)

		if err != nil {
			return fmt.Errorf("failed to write table content, table: %s, error: %v", table, err)
		}
	}

	err = m.writeFooter(buf)
	if err != nil {
		return fmt.Errorf("failed to write dump footer, error: %v", err)
	}

	return nil
}

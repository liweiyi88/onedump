package dumper

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/liweiyi88/onedump/config"
	"github.com/stretchr/testify/assert"
)

func initTest(t *testing.T) (*assert.Assertions, *sql.DB, sqlmock.Sqlmock) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	return assert, db, mock
}

func createTestMysqlNativeDump(db *sql.DB) *MysqlNativeDump {
	job := config.NewJob("test", "mysql", testDBDsn, config.WithSshHost("ssh"), config.WithSshKey("key"), config.WithSshUser("user"))
	mysql, _ := NewMysqlNativeDump(job)
	mysql.db = db

	return mysql
}

func TestNewMysqlDump(t *testing.T) {
	assert, _, _ := initTest(t)

	job := config.NewJob("test", "mysql", testDBDsn, config.WithSshHost("ssh"), config.WithSshKey("key"), config.WithSshUser("user"))
	_, err := NewMysqlNativeDump(job)

	assert.Nil(err)
}

func TestGetCharacterSet(t *testing.T) {
	assert, db, mock := initTest(t)
	mysql := createTestMysqlNativeDump(db)

	mock.ExpectQuery("SHOW VARIABLES LIKE 'character_set_database'").WillReturnError(errors.New("db err"))
	_, err := mysql.getCharacterSet()
	assert.NotNil(err)

	rows := mock.NewRows([]string{"variableName", "characterSet"}).AddRow("character_set_database", "utf8")

	mock.ExpectQuery("SHOW VARIABLES LIKE 'character_set_database'").WillReturnRows(rows)
	charset, err := mysql.getCharacterSet()
	assert.Nil(err)
	assert.Equal("utf8", charset)
}

func TestGetTables(t *testing.T) {
	assert, db, mock := initTest(t)
	mysql := createTestMysqlNativeDump(db)

	mock.ExpectQuery("SHOW TABLES").WillReturnError(errors.New("db err"))
	_, err := mysql.getTables()
	assert.NotNil(err)

	rows := mock.NewRows([]string{"tables"}).AddRow("onedump").AddRow("users")

	mock.ExpectQuery("SHOW TABLES").WillReturnRows(rows)

	tables, err := mysql.getTables()
	assert.Nil(err)
	assert.Equal([]string{"onedump", "users"}, tables)
}

func TestWriteTableContentOK(t *testing.T) {
	assert, db, mock := initTest(t)
	mysql := createTestMysqlNativeDump(db)

	mock.ExpectQuery("SELECT * FROM `onedump`;").WillReturnError(errors.New("failed to query table"))

	var b bytes.Buffer
	buf := bufio.NewWriter(&b)
	err := mysql.writeTableContent(buf, "onedump")
	assert.NotNil(err)

	rows := mock.
		NewRows([]string{"null", "unsigned_bigint", "tinyint", "tinyint_bytes", "float", "float_bytes", "decimal", "date", "datetime", "timestamp", "time", "year", "char", "binary", "varbinary", "bit", "tinyblob", "bool", "bool", "json"}).
		AddRow(nil, 1, 1, []byte("1"), 1.1, []byte("1.2"), 1.2, []uint8{50, 48, 50, 52, 45, 48, 54, 45, 49, 55}, []byte("2024-08-09 00:00:00"), []byte("1723161093"), []byte("00:00:00"), 2024, "char", []uint8{1}, "1", []uint8{1}, "tinyblob", true, false, []uint8{1}).
		AddRow(nil, 1, 1, []byte("1"), 1.1, []byte("1.2"), 1.2, []uint8{50, 48, 50, 52, 45, 48, 54, 45, 49, 55}, []byte("2024-08-09 00:00:00"), []byte("1723161093"), []byte("00:00:00"), 2024, "char", []uint8{1}, "1", []uint8{1}, "tinyblob", true, false, []uint8{1})

	val := reflect.ValueOf(rows).Elem()
	field := val.FieldByName("def")

	newColumns := []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("NULL", nil),
		sqlmock.NewColumn("new").OfType("UNSIGNED BIGINT", []uint8{1}),
		sqlmock.NewColumn("new").OfType("TINYINT", 1),
		sqlmock.NewColumn("new").OfType("TINYINT", 1),
		sqlmock.NewColumn("new").OfType("FLOAT", 1.1),
		sqlmock.NewColumn("new").OfType("FLOAT", 1.1),
		sqlmock.NewColumn("new").OfType("DECIMAL", 1.2),
		sqlmock.NewColumn("new").OfType("DATE", []uint8{50, 48, 50, 52, 45, 48, 54, 45, 49, 55}),
		sqlmock.NewColumn("new").OfType("DATETIME", "2024-08-09 00:00:00"),
		sqlmock.NewColumn("new").OfType("TIMESTAMP", "1723161093"),
		sqlmock.NewColumn("new").OfType("TIME", "00:00:00"),
		sqlmock.NewColumn("new").OfType("YEAR", 2024),
		sqlmock.NewColumn("new").OfType("CHAR", "char"),
		sqlmock.NewColumn("new").OfType("BINARY", []uint8{1}),
		sqlmock.NewColumn("new").OfType("VARBINARY", "1"),
		sqlmock.NewColumn("new").OfType("BIT", []uint8{1}),
		sqlmock.NewColumn("new").OfType("TINYBLOB", "tinyblob"),
		sqlmock.NewColumn("new").OfType("BOOL", true),
		sqlmock.NewColumn("new").OfType("BOOL", false),
		sqlmock.NewColumn("new").OfType("JSON", []uint8{1}),
	}

	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(newColumns))

	mock.ExpectQuery("SELECT * FROM `onedump`;").WillReturnRows(rows)

	err = mysql.writeTableContent(buf, "onedump")

	if err != nil {
		assert.Equal("unsupported database type: ", err.Error())
	}
}

func TestWriteTableContentInvalidColumnType(t *testing.T) {
	assert, db, mock := initTest(t)
	mysql := createTestMysqlNativeDump(db)

	mock.ExpectQuery("SELECT * FROM `onedump`;").WillReturnError(errors.New("failed to query table"))

	var b bytes.Buffer
	buf := bufio.NewWriter(&b)
	err := mysql.writeTableContent(buf, "onedump")
	assert.NotNil(err)

	testRows := make([]*sqlmock.Rows, 0)
	testRows = append(testRows, mock.NewRows([]string{"date"}).AddRow("1"))
	testRows = append(testRows, mock.NewRows([]string{"datetime"}).AddRow("1"))
	testRows = append(testRows, mock.NewRows([]string{"timestamp"}).AddRow("1"))
	testRows = append(testRows, mock.NewRows([]string{"time"}).AddRow("1"))
	testRows = append(testRows, mock.NewRows([]string{"year"}).AddRow("1"))
	testRows = append(testRows, mock.NewRows([]string{"binary"}).AddRow("1"))
	testRows = append(testRows, mock.NewRows([]string{"bit"}).AddRow("1"))
	testRows = append(testRows, mock.NewRows([]string{"bit"}).AddRow([]uint8{1, 2}))
	testRows = append(testRows, mock.NewRows([]string{"json"}).AddRow("1"))
	testRows = append(testRows, mock.NewRows([]string{"unsupport"}).AddRow("1"))

	var testColumns [][]*sqlmock.Column
	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("DATE", time.Now()),
	})

	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("DATETIME", time.Now()),
	})

	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("TIMESTAMP", time.Now()),
	})

	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("TIME", time.Now()),
	})

	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("YEAR", time.Now()),
	})

	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("BINARY", "1"),
	})

	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("BIT", "1"),
	})

	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("BIT", "12"),
	})

	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("JSON", "12"),
	})

	testColumns = append(testColumns, []*sqlmock.Column{
		sqlmock.NewColumn("new").OfType("UNSUPPORT", "12"),
	})

	want := []string{"could not parse DATE type, expect []uint8, got string",
		"could not parse DATETIME type, expect []byte, got string",
		"could not parse TIMESTAMP type, expect []byte, got string",
		"could not parse TIME type, expect []byte, got string",
		"could not parse YEAR type, expect int64, got string",
		"could not parse BINARY type, expect []uint8, got string",
		"cloud not parse BIT type, expect []uint8, got string",
		"failed to parse BIT type, expected length 1, got 2",
		"cloud not parse JSON type, expect []unint8, got string",
		"unsupported database type: UNSUPPORT",
	}

	for i, rows := range testRows {
		val := reflect.ValueOf(rows).Elem()
		field := val.FieldByName("def")

		reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).
			Elem().
			Set(reflect.ValueOf(testColumns[i]))

		mock.ExpectQuery("SELECT * FROM `onedump`;").WillReturnRows(rows)

		err = mysql.writeTableContent(buf, "onedump")

		if err != nil {
			assert.Equal(want[i], err.Error())
		}
	}
}

func TestWriteHeader(t *testing.T) {
	assert, db, mock := initTest(t)
	mysql := createTestMysqlNativeDump(db)

	rows := mock.NewRows([]string{"variableName", "characterSet"}).AddRow("chartset", "utf8")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'character_set_database'").WillReturnRows(rows)

	var b bytes.Buffer
	buf := bufio.NewWriter(&b)

	err := mysql.writeHeader(buf)
	assert.Nil(err)
}

func TestWriteFooter(t *testing.T) {
	assert, db, _ := initTest(t)
	mysql := createTestMysqlNativeDump(db)

	var b bytes.Buffer
	buf := bufio.NewWriter(&b)

	err := mysql.writeFooter(buf)
	assert.Nil(err)
}

func TestWriteTableStructure(t *testing.T) {
	assert, db, mock := initTest(t)
	mysql := createTestMysqlNativeDump(db)

	rows := mock.NewRows([]string{"name", "createTable"}).AddRow("name", "table_structure")
	mock.ExpectQuery("SHOW CREATE TABLE `onedump`").WillReturnRows(rows)

	var b bytes.Buffer
	buf := bufio.NewWriter(&b)

	err := mysql.writeTableStructure(buf, "onedump")
	assert.Nil(err)
}

package slow

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser(t *testing.T) {
	Parse("testutils/slowlog_mysql.log", MySQL, 0, true)
}

func TestGetParser(t *testing.T) {
	_, err := getParser("unknown")
	assert.Equal(t, "unsupported database type: unknown", err.Error())

	parser, err := getParser(MySQL)
	assert.Nil(t, err)
	assert.Equal(t, parser, NewMySQLSlowLogParser())
}

func TestParseOneFile(t *testing.T) {
	assert := assert.New(t)
	result := Parse("../testutils/slowlog_mysql.log", MySQL, 0, false)
	assert.True(result.OK)
	assert.Equal(result.Error, "")
	assert.Len(result.Results, 3)
}

func TestParseOneGzippedFile(t *testing.T) {
	assert := assert.New(t)
	result := Parse("../testutils/slowlog_mysql.log.gz", MySQL, 0, false)
	assert.True(result.OK)
	assert.Equal(result.Error, "")
	assert.Len(result.Results, 3)
}

func TestParseDirectory(t *testing.T) {
	assert := assert.New(t)
	result := Parse("../testutils", MySQL, 0, false)

	assert.True(result.OK)
	assert.Equal(result.Error, "")
	assert.Len(result.Results, 7)
	expected := `{"ok":true,"error":"","results":[{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host":"192.168.1.101","query_time":12.890323,"lock_time":0.001456,"rows_sent":100,"rows_examined":100000,"query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > 10"},{"time":"2023-10-15T12:34:56.123456Z","user":"root[root]","host":"localhost","query_time":9.123456,"lock_time":0.001234,"rows_sent":10,"rows_examined":10000,"query":"SELECT * FROM orders WHERE customer_id = 123 AND order_date > '2023-01-01' ORDER BY order_date DESC"},{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host":"192.168.1.101","query_time":7.890123,"lock_time":0.003456,"rows_sent":100,"rows_examined":100000,"query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > 10"},{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host":"192.168.1.101","query_time":7.890123,"lock_time":0.003456,"rows_sent":100,"rows_examined":100000,"query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > 10"},{"time":"2023-10-15T12:34:56.123456Z","user":"root[root]","host":"localhost","query_time":5.123456,"lock_time":0.001234,"rows_sent":10,"rows_examined":10000,"query":"SELECT * FROM orders WHERE customer_id = 123 AND order_date > '2023-01-01' ORDER BY order_date DESC"},{"time":"2023-10-15T12:35:10.654321Z","user":"app_user[app_user]","host":"192.168.1.100","query_time":3.456789,"lock_time":0.002345,"rows_sent":1,"rows_examined":5000,"query":"UPDATE products SET stock = stock - 1 WHERE product_id = 456"},{"time":"2023-10-15T12:35:10.654321Z","user":"app_user[app_user]","host":"192.168.1.100","query_time":3.456789,"lock_time":0.002345,"rows_sent":1,"rows_examined":5000,"query":"UPDATE products SET stock = stock - 1 WHERE product_id = 456"}]}`

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)

	err := encoder.Encode(result)

	if err != nil {
		t.Error(err)
	}

	assert.Equal(expected, strings.TrimSpace(buffer.String()))
}

func TestParseDirectoryWithLimit(t *testing.T) {
	assert := assert.New(t)
	result := Parse("../testutils", MySQL, 2, false)

	assert.True(result.OK)
	assert.Equal(result.Error, "")
	assert.Len(result.Results, 2)
	expected := `{"ok":true,"error":"","results":[{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host":"192.168.1.101","query_time":12.890323,"lock_time":0.001456,"rows_sent":100,"rows_examined":100000,"query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > 10"},{"time":"2023-10-15T12:34:56.123456Z","user":"root[root]","host":"localhost","query_time":9.123456,"lock_time":0.001234,"rows_sent":10,"rows_examined":10000,"query":"SELECT * FROM orders WHERE customer_id = 123 AND order_date > '2023-01-01' ORDER BY order_date DESC"}]}`

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)

	err := encoder.Encode(result)

	if err != nil {
		t.Error(err)
	}

	assert.Equal(expected, strings.TrimSpace(buffer.String()))
}

func TestParseDirectoryWithLimitMask(t *testing.T) {
	assert := assert.New(t)
	result := Parse("../testutils", MySQL, 2, true)

	assert.True(result.OK)
	assert.Equal(result.Error, "")
	assert.Len(result.Results, 2)
	expected := `{"ok":true,"error":"","results":[{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host":"192.168.1.101","query_time":12.890323,"lock_time":0.001456,"rows_sent":100,"rows_examined":100000,"query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > ?"},{"time":"2023-10-15T12:34:56.123456Z","user":"root[root]","host":"localhost","query_time":9.123456,"lock_time":0.001234,"rows_sent":10,"rows_examined":10000,"query":"SELECT * FROM orders WHERE customer_id = ? AND order_date > ? ORDER BY order_date DESC"}]}`

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)

	err := encoder.Encode(result)

	if err != nil {
		t.Error(err)
	}

	assert.Equal(expected, strings.TrimSpace(buffer.String()))
}

func TestParseInvalidFile(t *testing.T) {
	assert := assert.New(t)
	result := Parse("nonexistent.log", MySQL, 0, false)
	assert.False(result.OK)
	assert.NotNil(result.Error)
	assert.Empty(result.Results)
}

func TestParseCorruptedGzip(t *testing.T) {
	assert := assert.New(t)
	result := Parse("../testutils/corrupted.log.gz", MySQL, 0, false)
	assert.False(result.OK)
	assert.NotNil(result.Error)
	assert.Empty(result.Results)
}

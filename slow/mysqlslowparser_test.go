package slow

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLParseFailure(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedErr string
	}{
		{
			name:        "No time match",
			input:       "# Time: invalid-time-format",
			expectedErr: "fail to parse time",
		},
		{
			name:        "No user match",
			input:       "# Time: 2023-10-15T12:34:56.123456Z\n# User@Host: [app_user] @ [192.168.1.100]",
			expectedErr: "fail to parse user and host, line: # User@Host: [app_user] @ [192.168.1.100]",
		},
		{
			name:        "Invalid query time",
			input:       "# Time: 2023-10-15T12:34:56.123456Z\n# User@Host: app_user[app_user] @ [192.168.1.100] \n# Query_time: invalid-query  Lock_time: 0.003456 Rows_sent: 100  Rows_examined: 100000",
			expectedErr: "fail to convert Query_time to float64",
		},
	}

	parser := NewMySQLSlowLogParser()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			_, err := parser.parse(reader)

			if err == nil {
				t.Errorf("Expected error, got nil")
			} else if err.Error() != tt.expectedErr {
				t.Errorf("Expected error: %v, got: %v", tt.expectedErr, err)
			}
		})
	}
}

func TestMySQLParse(t *testing.T) {
	parser := NewMySQLSlowLogParser()

	file, err := os.Open("../testutils/slowlogs/short/slowlog_mysql.log")

	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	_, err = parser.parse(file)

	if err != nil {
		t.Error(err)
	}

	// Test multiple parse calls should still return the expected result
	file.Seek(0, 0)
	results, err := parser.parse(file)
	if err != nil {
		t.Error(err)
	}

	assert.Len(t, results, 3)

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)

	err = encoder.Encode(results)

	if err != nil {
		t.Error(err)
	}

	expect := `[{"time":"2023-10-15T12:34:56.123456Z","user":"root[root]","host_ip":"localhost []","query_time":9.123456,"lock_time":0.001234,"rows_sent":10,"rows_examined":10000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT * FROM orders WHERE customer_id = 123 AND order_date > '2023-01-01' ORDER BY order_date DESC"},{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host_ip":"[192.168.1.101]","query_time":7.890123,"lock_time":0.003456,"rows_sent":100,"rows_examined":100000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > 10"},{"time":"2023-10-15T12:35:10.654321Z","user":"app_user[app_user]","host_ip":"[192.168.1.100]","query_time":3.456789,"lock_time":0.002345,"rows_sent":1,"rows_examined":5000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"UPDATE products SET stock = stock - 1 WHERE product_id = 456"}]`
	assert.Equal(t, expect, strings.TrimSpace(buffer.String()))
}

func TestDeduplicationParseWithoutMask(t *testing.T) {
	parser := NewMySQLSlowLogParser()

	file, err := os.Open("../testutils/slowlogs/short/slowlog_mysql_duplicated.log")

	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	results, err := parser.parse(file)

	if err != nil {
		t.Error(err)
	}

	assert.Len(t, results, 1)

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)

	err = encoder.Encode(results)

	if err != nil {
		t.Error(err)
	}

	expect := `[{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host_ip":"[192.168.1.101]","query_time":12.890323,"lock_time":0.001456,"rows_sent":100,"rows_examined":100000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > 10"}]`
	assert.Equal(t, expect, strings.TrimSpace(buffer.String()))
}

func TestDeduplicationParseWithMask(t *testing.T) {
	parser := NewMySQLSlowLogParser()
	parser.setMask(true)

	file, err := os.Open("../testutils/slowlogs/short/slowlog_mysql_duplicated.log")

	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	results, err := parser.parse(file)

	if err != nil {
		t.Error(err)
	}

	assert.Len(t, results, 1)

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(results)

	if err != nil {
		t.Error(err)
	}

	expect := `[{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host_ip":"[192.168.1.101]","query_time":12.890323,"lock_time":0.001456,"rows_sent":100,"rows_examined":100000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > ?"}]`
	assert.Equal(t, expect, strings.TrimSpace(buffer.String()))
}

type ErrorReader struct{}

func (e *ErrorReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

func TestScannerErr(t *testing.T) {
	parser := NewMySQLSlowLogParser()
	reader := &ErrorReader{} // use the error-producing reader

	_, err := parser.parse(reader)

	if err == nil || !strings.Contains(err.Error(), "simulated read error") {
		t.Errorf("expected error containing 'simulated read error', got %v", err)
	}
}

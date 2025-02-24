package cmd

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlowCmd(t *testing.T) {
	// Skip this test on Windows
	if runtime.GOOS == "windows" {
		t.Skip("Skipping test on Windows")
	}

	originalStdout := os.Stdout
	r, w, _ := os.Pipe()                          // Create a pipe to capture stdout
	os.Stdout = w                                 // Redirect stdout
	defer func() { os.Stdout = originalStdout }() // Ensure that os.Stdout is restored after the test

	assert := assert.New(t)
	cmd := rootCmd
	cmd.SetArgs([]string{"slow", "-f", "../testutils/slowlogs/short"})
	cmd.Execute()

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)

	// Get the captured output
	out := buf.String()

	expected := `{"ok":true,"error":"","results":[{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host_ip":"[192.168.1.101]","query_time":12.890323,"lock_time":0.001456,"rows_sent":100,"rows_examined":100000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > ?"},{"time":"2023-10-15T12:34:56.123456Z","user":"root[root]","host_ip":"localhost []","query_time":9.123456,"lock_time":0.001234,"rows_sent":10,"rows_examined":10000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT * FROM orders WHERE customer_id = ? AND order_date > ? ORDER BY order_date DESC"},{"time":"2023-10-15T12:34:56.123456Z","user":"root[root]","host_ip":"localhost []","query_time":9.123456,"lock_time":0.001234,"rows_sent":10,"rows_examined":10000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT * FROM orders WHERE customer_id = ? AND order_date > ? ORDER BY order_date DESC"},{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host_ip":"[192.168.1.101]","query_time":7.890123,"lock_time":0.003456,"rows_sent":100,"rows_examined":100000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > ?"},{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host_ip":"[192.168.1.101]","query_time":7.890123,"lock_time":0.003456,"rows_sent":100,"rows_examined":100000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > ?"},{"time":"2023-10-15T12:35:10.654321Z","user":"app_user[app_user]","host_ip":"[192.168.1.100]","query_time":3.456789,"lock_time":0.002345,"rows_sent":1,"rows_examined":5000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"UPDATE products SET stock = stock - ? WHERE product_id = ?"},{"time":"2023-10-15T12:35:10.654321Z","user":"app_user[app_user]","host_ip":"[192.168.1.100]","query_time":3.456789,"lock_time":0.002345,"rows_sent":1,"rows_examined":5000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"UPDATE products SET stock = stock - ? WHERE product_id = ?"}]}`
	assert.Equal(expected, strings.TrimSpace(string(out)))
}

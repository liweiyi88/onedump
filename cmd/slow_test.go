package cmd

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlowCmd(t *testing.T) {
	originalStdout := os.Stdout
	r, w, _ := os.Pipe()                          // Create a pipe to capture stdout
	os.Stdout = w                                 // Redirect stdout
	defer func() { os.Stdout = originalStdout }() // Ensure that os.Stdout is restored after the test

	assert := assert.New(t)
	cmd := rootCmd
	cmd.SetArgs([]string{"slow", "-f", "../testutils"})
	cmd.Execute()

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)

	// Get the captured output
	out := buf.String()

	expected := `{"ok":true,"error":null,"results":[{"time":"2023-10-15T12:34:56.123456Z","user":"root[root]","host":"localhost","query_time":9.123456,"lock_time":0.001234,"rows_sent":10,"rows_examined":10000,"query":"SELECT * FROM orders WHERE customer_id = ? AND order_date > ? ORDER BY order_date DESC"},{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host":"192.168.1.101","query_time":7.890123,"lock_time":0.003456,"rows_sent":100,"rows_examined":100000,"query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > ?"},{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host":"192.168.1.101","query_time":7.890123,"lock_time":0.003456,"rows_sent":100,"rows_examined":100000,"query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > ?"},{"time":"2023-10-15T12:34:56.123456Z","user":"root[root]","host":"localhost","query_time":5.123456,"lock_time":0.001234,"rows_sent":10,"rows_examined":10000,"query":"SELECT * FROM orders WHERE customer_id = ? AND order_date > ? ORDER BY order_date DESC"},{"time":"2023-10-15T12:35:10.654321Z","user":"app_user[app_user]","host":"192.168.1.100","query_time":3.456789,"lock_time":0.002345,"rows_sent":1,"rows_examined":5000,"query":"UPDATE products SET stock = stock - ? WHERE product_id = ?"},{"time":"2023-10-15T12:35:10.654321Z","user":"app_user[app_user]","host":"192.168.1.100","query_time":3.456789,"lock_time":0.002345,"rows_sent":1,"rows_examined":5000,"query":"UPDATE products SET stock = stock - ? WHERE product_id = ?"}]}`
	assert.Equal(expected, strings.TrimSpace(string(out)))
}

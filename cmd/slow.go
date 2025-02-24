package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liweiyi88/onedump/slow"
	"github.com/spf13/cobra"
)

func isValidDatabase(db string) bool {
	return db == string(slow.MySQL) || db == string(slow.PostgreSQL)
}

var slowCmd = &cobra.Command{
	Use:   "slow",
	Short: "Database slow log parser",
	Long:  "Database slow log parser, it formats the result in json",
	RunE: func(cmd *cobra.Command, args []string) error {
		if !isValidDatabase(database) {
			return fmt.Errorf("unsupported database type: %s, support [mysql]", database)
		}

		databaseType := slow.DatabaseType(database)

		result := slow.Parse(sloglog, databaseType, limit, mask)

		var buffer bytes.Buffer

		encoder := json.NewEncoder(&buffer)
		encoder.SetEscapeHTML(false)

		err := encoder.Encode(result)
		fmt.Println(strings.TrimSpace(buffer.String()))
		return err
	},
}

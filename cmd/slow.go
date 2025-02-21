package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liweiyi88/onedump/slow"
	"github.com/spf13/cobra"
)

var sloglog, database string
var limit int
var mask bool

var slowCmd = &cobra.Command{
	Use:   "slow",
	Short: "Database slow log parser",
	Long:  "Database slow log parser, it formats the result in json",
	RunE: func(cmd *cobra.Command, args []string) error {
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

package slowcmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/liweiyi88/onedump/slow"
	"github.com/spf13/cobra"
)

var (
	sloglog, database, pattern string
	limit                      int
	mask                       bool
)

func init() {
	SlowCmd.Flags().StringVarP(&sloglog, "file", "f", "", "path to the slow log file. a directory can also be specified. (required)")
	SlowCmd.Flags().StringVarP(&database, "database", "d", string(slow.MySQL), "specify the database engine (optional)")
	SlowCmd.Flags().StringVarP(&pattern, "pattern", "p", "", "only read files that follow the same pattern, for example *slow.log . (optional)")
	SlowCmd.Flags().IntVarP(&limit, "limit", "l", 0, "limit the number of results. no limit is set by default. (optional)")
	SlowCmd.Flags().BoolVarP(&mask, "mask", "m", true, "mask query values. enabled by default. (optional)")
	SlowCmd.MarkFlagRequired("file")
}

func isValidDatabase(db string) bool {
	return db == string(slow.MySQL) || db == string(slow.PostgreSQL)
}

var SlowCmd = &cobra.Command{
	Use:   "slow",
	Short: "Database slow log parser",
	Long:  "Database slow log parser, it formats the result in json",
	RunE: func(cmd *cobra.Command, args []string) error {
		if !isValidDatabase(database) {
			return fmt.Errorf("unsupported database type: %s, support [mysql]", database)
		}

		databaseType := slow.DatabaseType(database)
		result := slow.Parse(sloglog, databaseType, slow.ParseOptions{Limit: limit, Mask: mask, Pattern: pattern})

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetEscapeHTML(false)

		return encoder.Encode(result)
	},
}

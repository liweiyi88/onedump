package slow

import (
	"fmt"
	"io"
	"os"
)

type Parser interface {
	parse(io.Reader) ([]SlowResult, error)
}

type SlowResult struct {
	Time         string  `json:"time"`
	User         string  `json:"user"`
	Host         string  `json:"host"`
	QueryTime    float64 `json:"query_time"`
	LockTime     float64 `json:"lock_time"`
	RowsSent     uint    `json:"rows_sent"`
	RowsExamined uint    `json:"rows_examined"`
	Query        string  `json:"query"`
}

type ParseResult struct {
	Ok      bool
	Error   error
	Results []SlowResult
}

type DatabaseType string

const (
	MySQL      DatabaseType = "mysql"
	PostgreSQL DatabaseType = "postgresql"
)

func getParser(database DatabaseType) (Parser, error) {
	switch database {
	case MySQL:
		return NewMySQLSlowLogParser(), nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", database)
	}
}

func Parse(filePath string, database DatabaseType) ([]SlowResult, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}

	parser, err := getParser(database)
	if err != nil {
		return nil, err
	}

	if !fileInfo.IsDir() {
		file, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open file, error: %v", err)
		}

		return parser.parse(file)
	}

	//TODO: Loop through the dir and read file, use go routine

	return nil, nil
}

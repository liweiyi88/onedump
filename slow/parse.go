package slow

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"sync"

	"github.com/liweiyi88/onedump/fileutil"
)

type Parser interface {
	parse(io.Reader) ([]SlowResult, error)
	setMask(bool)
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
	OK      bool         `json:"ok"`
	Error   error        `json:"error"`
	Results []SlowResult `json:"results"`
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

func maskQuery(query string) string {
	numberRegex := regexp.MustCompile(`\b\d+\b`)
	maskedQuery := numberRegex.ReplaceAllString(query, "?")

	// Regex to match strings enclosed in single quotes
	singleQuoteRegex := regexp.MustCompile(`'[^']*'`)
	maskedQuery = singleQuoteRegex.ReplaceAllString(maskedQuery, "?")

	// Regex to match strings enclosed in double quotes
	doubleQuoteRegex := regexp.MustCompile(`"[^"]*"`)
	maskedQuery = doubleQuoteRegex.ReplaceAllString(maskedQuery, "?")

	return maskedQuery
}

func parseFile(filePath string, database DatabaseType, mask bool) ([]SlowResult, error) {
	parser, err := getParser(database)
	if err != nil {
		return nil, err
	}

	parser.setMask(mask)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("fail to open file, error: %v", err)
	}

	defer func() {
		err := file.Close()

		if err != nil {
			slog.Error("fail to close file when parse slow log", slog.Any("error", err))
		}
	}()

	if !fileutil.IsGzipped(filePath) {
		return parser.parse(file)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("fail to create gzip reader, error: %v", err)
	}

	defer func() {
		if err := gzipReader.Close(); err != nil {
			slog.Error("fail to close gzip reader when parse slow log", slog.Any("error", err))
		}
	}()

	return parser.parse(gzipReader)
}

func Parse(filePath string, database DatabaseType, limit int, mask bool) ParseResult {
	parseResult := ParseResult{}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		parseResult.Error = err
		return parseResult
	}

	if !fileInfo.IsDir() {
		results, err := parseFile(filePath, database, mask)
		if err != nil {
			parseResult.Error = err
			return parseResult
		}

		if limit > 0 && len(results) > limit {
			results = results[:limit]
		}

		// Already sorted by query time in the parseFile function
		parseResult.Results = results
		parseResult.OK = true
		return parseResult
	}

	files, err := fileutil.ListFiles(filePath)

	if err != nil {
		parseResult.Error = err
		return parseResult
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	results, errorsList := make([]SlowResult, 0), make([]error, 0)

	for _, filename := range files {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			r, err := parseFile(name, database, mask)

			mu.Lock()
			if err != nil {
				errorsList = append(errorsList, err)
			}

			results = append(results, r...)
			mu.Unlock()
		}(filename)
	}

	wg.Wait()

	if len(errorsList) > 0 {
		parseResult.Error = errors.Join(errorsList...)
	} else {
		parseResult.OK = true
	}

	// Need this further sorting on the whole results
	sort.Slice(results, func(i, j int) bool {
		return results[i].QueryTime > results[j].QueryTime
	})

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	parseResult.Results = results
	return parseResult
}

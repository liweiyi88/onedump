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

type SlowResult struct {
	Time                 string  `json:"time"`
	User                 string  `json:"user"`
	HostIP               string  `json:"host_ip"`
	QueryTime            float64 `json:"query_time"`
	LockTime             float64 `json:"lock_time"`
	RowsSent             int     `json:"rows_sent"`
	RowsExamined         int     `json:"rows_examined"`
	ThreadId             int     `json:"thread_id"`
	Errno                int     `json:"errno"`
	Killed               int     `json:"killed"`
	BytesReceived        int     `json:"bytes_received"`
	BytesSent            int     `json:"bytes_sent"`
	ReadFirst            int     `json:"read_first"`
	ReadLast             int     `json:"read_last"`
	ReadKey              int     `json:"read_key"`
	ReadNext             int     `json:"read_next"`
	ReadPrev             int     `json:"read_prev"`
	ReadRnd              int     `json:"read_rnd"`
	ReadRndNext          int     `json:"read_rnd_next"`
	SortMergePasses      int     `json:"sort_merge_passes"`
	SortRangeCount       int     `json:"sort_range_count"`
	SortRows             int     `json:"sort_rows"`
	SortScanCount        int     `json:"sort_scan_count"`
	CreatedTmpDiskTables int     `json:"created_tmp_disk_tables"`
	CreatedTmpTables     int     `json:"created_tmp_tables"`
	CountHitTmpTableSize int     `json:"count_hit_tmp_table_size"`
	Start                string  `json:"start"`
	End                  string  `json:"end"`
	Query                string  `json:"query"`
}

type Parser interface {
	parse(io.Reader) ([]SlowResult, error)
	setMask(bool)
}

type ParseResult struct {
	OK      bool         `json:"ok"`
	Error   string       `json:"error"`
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
		parseResult.Error = err.Error()
		return parseResult
	}

	if !fileInfo.IsDir() {
		results, err := parseFile(filePath, database, mask)
		if err != nil {
			parseResult.Error = err.Error()
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
		parseResult.Error = err.Error()
		return parseResult
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	results, errorsList := make([]SlowResult, 0), make([]error, 0)

	// Process maximum 10 files at a time
	semaphore := make(chan struct{}, 10)

	for _, filename := range files {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(name string) {
			defer func() {
				<-semaphore
				wg.Done()
			}()

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
		parseResult.Error = errors.Join(errorsList...).Error()
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

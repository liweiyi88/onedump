package slow

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

const (
	QueryTime            = "Query_time"
	LockTime             = "Lock_time"
	RowsSent             = "Rows_sent"
	RowsExamined         = "Rows_examined"
	ThreadId             = "Thread_id"
	Errno                = "Errno"
	Killed               = "Killed"
	BytesReceived        = "Bytes_received"
	BytesSent            = "Bytes_sent"
	ReadFirst            = "Read_first"
	ReadLast             = "Read_last"
	ReadKey              = "Read_key"
	ReadNext             = "Read_next"
	ReadPrev             = "Read_prev"
	ReadRnd              = "Read_rnd"
	ReadRndNext          = "Read_rnd_next"
	SortMergePasses      = "Sort_merge_passes"
	SortRangeCount       = "Sort_range_count"
	SortRows             = "Sort_rows"
	SortScanCount        = "Sort_scan_count"
	CreatedTmpDiskTables = "Created_tmp_disk_tables"
	CreatedTmpTables     = "Created_tmp_tables"
	CountHitTmpTableSize = "Count_hit_tmp_table_size"
	Start                = "Start"
	End                  = "End"
)

var timeRegex = regexp.MustCompile(`(?i)^# Time: (\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+\d{2}:?\d{2})?)`)
var userHostRegex = regexp.MustCompile(`(?i)^# User@Host: (\S+\[\S+\]) @ ([\w\.\-]*\s*\[.*?\])`)

type MySQLSlowLogParser struct {
	result          *SlowResult
	query           strings.Builder
	queryResultsMap map[string]SlowResult
	mask            bool
	startQuery      bool
}

func NewMySQLSlowLogParser() *MySQLSlowLogParser {
	return &MySQLSlowLogParser{}
}

func (m *MySQLSlowLogParser) flush() {
	if m.result != nil && m.result.QueryTime > 0 {
		rawQuery := strings.TrimSpace(m.query.String())
		query := strings.ReplaceAll(rawQuery, ";", "")

		if m.mask {
			query = maskQuery(query)
		}

		m.result.Query = query
		savedResult, ok := m.queryResultsMap[query]

		if len(query) > 0 && (!ok || savedResult.QueryTime < m.result.QueryTime) {
			m.queryResultsMap[query] = *m.result
		}
	}

	m.reset()
}

func (m *MySQLSlowLogParser) init() {
	m.queryResultsMap = make(map[string]SlowResult)
	m.reset()
}

func (m *MySQLSlowLogParser) parseLineHasTimeData(line string) error {
	if strings.HasPrefix(line, "# Time") {
		m.flush()

		m.result = &SlowResult{}
		matches := timeRegex.FindStringSubmatch(line)

		if len(matches) <= 1 {
			return errors.New("fail to parse time")
		}

		m.result.Time = matches[1]
	}

	return nil
}

func (m *MySQLSlowLogParser) parseLineHasUserHostData(line string) error {
	if m.result != nil && strings.HasPrefix(line, "# User") {
		matches := userHostRegex.FindStringSubmatch(line)

		if len(matches) <= 2 {
			return fmt.Errorf("fail to parse user and host, line: %s", line)
		}

		userDb, hostIp := matches[1], matches[2]

		m.result.User = userDb
		m.result.HostIP = strings.TrimSpace(hostIp)
	}

	return nil
}

func (m *MySQLSlowLogParser) parseLineHasPerformanceData(line string) error {
	if m.result != nil && strings.HasPrefix(line, "# Query_time") {
		metricsToFloat := []string{QueryTime, LockTime}
		metricsToInt := []string{RowsSent, RowsExamined, ThreadId, Errno,
			Killed, BytesReceived, BytesSent, ReadFirst, ReadLast, ReadKey,
			ReadNext, ReadPrev, ReadRnd, ReadRndNext, SortMergePasses, SortRangeCount,
			SortRows, SortScanCount, CreatedTmpDiskTables, CreatedTmpTables, CountHitTmpTableSize}

		// Get rid of # and split the line into different chunks by space.
		chunks := strings.Fields(line[2:])

		for i := 0; i < len(chunks); i++ {
			if i+1 < len(chunks) {
				metric := strings.TrimSpace(strings.TrimSuffix(chunks[i], ":"))
				value := strings.TrimSpace(chunks[i+1])

				if slices.Contains(metricsToFloat, metric) {
					value, err := strconv.ParseFloat(value, 64)
					if err != nil {
						return fmt.Errorf("fail to convert %s to float64", metric)
					}

					switch metric {
					case QueryTime:
						m.result.QueryTime = value
					case LockTime:
						m.result.LockTime = value
					}
				} else if slices.Contains(metricsToInt, metric) {
					value, err := strconv.Atoi(value)
					if err != nil {
						return fmt.Errorf("fail to convert %s to int", metric)
					}

					switch metric {
					case RowsSent:
						m.result.RowsSent = value
					case RowsExamined:
						m.result.RowsExamined = value
					case ThreadId:
						m.result.ThreadId = value
					case Errno:
						m.result.Errno = value
					case Killed:
						m.result.Killed = value
					case BytesReceived:
						m.result.BytesReceived = value
					case BytesSent:
						m.result.BytesSent = value
					case ReadFirst:
						m.result.ReadFirst = value
					case ReadLast:
						m.result.ReadLast = value
					case ReadKey:
						m.result.ReadKey = value
					case ReadNext:
						m.result.ReadNext = value
					case ReadPrev:
						m.result.ReadPrev = value
					case ReadRnd:
						m.result.ReadRnd = value
					case ReadRndNext:
						m.result.ReadRndNext = value
					case SortMergePasses:
						m.result.SortMergePasses = value
					case SortRangeCount:
						m.result.SortRangeCount = value
					case SortRows:
						m.result.SortRows = value
					case SortScanCount:
						m.result.SortScanCount = value
					case CreatedTmpDiskTables:
						m.result.CreatedTmpDiskTables = value
					case CreatedTmpTables:
						m.result.CreatedTmpTables = value
					case CountHitTmpTableSize:
						m.result.CountHitTmpTableSize = value
					}
				} else {
					switch metric {
					case Start:
						m.result.Start = value
					case End:
						m.result.End = value
					}
				}
			}
		}
	}

	return nil
}

func (m *MySQLSlowLogParser) reset() {
	m.result = nil
	m.query.Reset()
	m.startQuery = false
}

// setMask enables or disables query masking.
// When enabled, sensitive data in queries will be replaced with ?.
func (m *MySQLSlowLogParser) setMask(mask bool) {
	m.mask = mask
}

func (m *MySQLSlowLogParser) shouldCaptureQuery(line string) bool {
	// Comments starts with --, metadata starts with #
	// We should skip capture those lines as query
	return m.startQuery && !strings.HasPrefix(line, "--") && !strings.HasPrefix(line, "#")
}

func (m *MySQLSlowLogParser) parse(file io.Reader) ([]SlowResult, error) {
	m.init()

	// Use reader.ReaderString('\n') to read long lines.
	// Scaner will return error if line is too long as it has fixed buffer size, default is 64KB.
	// Although we can adjust the buffer size of Scanner, we just keep it simple here.
	reader := bufio.NewReader(file)

	var eof bool

	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			if err != io.EOF {
				return nil, err
			}

			eof = true
		}

		err = m.parseLineHasTimeData(line)
		if err != nil {
			return nil, err
		}

		if m.shouldCaptureQuery(line) {
			m.query.WriteString(line)

			if eof {
				break
			}

			continue
		}

		err = m.parseLineHasUserHostData(line)
		if err != nil {
			return nil, err
		}

		err = m.parseLineHasPerformanceData(line)
		if err != nil {
			return nil, err
		}

		if m.result != nil && strings.HasPrefix(line, "SET") {
			m.startQuery = true
		}

		// Make sure we break the loop when reading invalid slow query log file
		if eof {
			break
		}
	}

	m.flush()

	results := make([]SlowResult, 0, len(m.queryResultsMap))

	for _, result := range m.queryResultsMap {
		results = append(results, result)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].QueryTime > results[j].QueryTime
	})

	return results, nil
}

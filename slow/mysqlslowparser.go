package slow

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var timeRegex = regexp.MustCompile(`(?i)^# Time: (\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+\d{2}:?\d{2})?)`)
var userHostRegex = regexp.MustCompile(`(?i)^# User@Host: (\S+\[\S+\]) @ ([\w\.\-]+)(?: \[(\d{1,3}(?:\.\d{1,3}){3}|[a-fA-F0-9:]+(?::\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})?)?\])?`)
var performanceRegex = regexp.MustCompile(`(?i)^# Query_time:\s+([\d.]+)\s+Lock_time:\s+([\d.]+)\s+Rows_sent:\s+(\d+)\s+Rows_examined:\s+(\d+)`)

type MySQLSlowLogParser struct {
	result          *SlowResult
	query           strings.Builder
	queryResultsMap map[string]SlowResult
	mask            bool
	captureQuery    bool
}

func NewMySQLSlowLogParser() *MySQLSlowLogParser {
	return &MySQLSlowLogParser{}
}

func (m *MySQLSlowLogParser) flush() {
	if m.result == nil {
		return
	}

	if m.result.QueryTime > 0 {
		rawQuery := strings.TrimSpace(m.query.String())
		query := strings.ReplaceAll(rawQuery, ";", "")

		if m.mask {
			query = maskQuery(query)
		}

		m.result.Query = query

		savedResult, ok := m.queryResultsMap[query]

		if !ok || savedResult.QueryTime < m.result.QueryTime {
			m.queryResultsMap[query] = *m.result
		}
	}

	m.reset()
}

func (m *MySQLSlowLogParser) init() {
	m.queryResultsMap = make(map[string]SlowResult)
	m.reset()
}

func (m *MySQLSlowLogParser) reset() {
	m.result = nil
	m.query.Reset()
	m.captureQuery = false
}

func (m *MySQLSlowLogParser) setResultPerformance(queryTime, lockTime float64, rowsSent, rowsExamined uint) {
	if m.result == nil {
		return
	}

	m.result.QueryTime = queryTime
	m.result.LockTime = lockTime
	m.result.RowsSent = rowsSent
	m.result.RowsExamined = rowsExamined
}

// setMask enables or disables query masking.
// When enabled, sensitive data in queries will be replaced with ?.
func (m *MySQLSlowLogParser) setMask(mask bool) {
	m.mask = mask
}

func (m *MySQLSlowLogParser) parse(file io.Reader) ([]SlowResult, error) {
	m.init()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "# Time") {
			m.flush()

			m.result = &SlowResult{}
			matches := timeRegex.FindStringSubmatch(line)

			if len(matches) <= 1 {
				return nil, fmt.Errorf("fail to parse time")
			}

			m.result.Time = matches[1]
		}

		if m.captureQuery {
			m.query.WriteString(line)
			continue
		}

		if m.result != nil && strings.HasPrefix(line, "# User") {
			matches := userHostRegex.FindStringSubmatch(line)

			if len(matches) <= 2 {
				return nil, fmt.Errorf("fail to parse user and host")
			}

			userDb, hostIp := matches[1], matches[2]

			m.result.User = userDb
			m.result.Host = hostIp
		}

		if m.result != nil && strings.HasPrefix(line, "# Query_time") {
			matches := performanceRegex.FindStringSubmatch(line)

			if len(matches) <= 4 {
				return nil, fmt.Errorf("fail to parse query performance data")
			}

			queryTime, err := strconv.ParseFloat(matches[1], 64)

			if err != nil {
				return nil, fmt.Errorf("fail to convert query time")
			}

			lockTime, err := strconv.ParseFloat(matches[2], 64)
			if err != nil {
				return nil, fmt.Errorf("fail to convert lock time")
			}

			rowsSent, err := strconv.Atoi(matches[3])
			if err != nil {
				return nil, fmt.Errorf("fail to convert rows sent")
			}

			rowsExamined, err := strconv.Atoi(matches[4])
			if err != nil {
				return nil, fmt.Errorf("fail to convert rows examined")
			}

			m.setResultPerformance(queryTime, lockTime, uint(rowsSent), uint(rowsExamined))
		}

		if m.result != nil && strings.HasPrefix(line, "SET") {
			m.captureQuery = true
		}
	}

	m.flush()

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	results := make([]SlowResult, 0, len(m.queryResultsMap))

	for _, result := range m.queryResultsMap {
		results = append(results, result)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].QueryTime > results[j].QueryTime
	})

	return results, nil
}

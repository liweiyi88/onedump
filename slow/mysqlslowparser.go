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

var timeRegex = regexp.MustCompile(`(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)`)
var userHostRegex = regexp.MustCompile(`(\S+\[\S+\])\s*@\s*(\S+(\s*\[\d+\.\d+\.\d+\.\d+\])?)`)
var performanceRegex = regexp.MustCompile(`Query_time:\s*([\d.]+)\s*Lock_time:\s*([\d.]+)\s*Rows_sent:\s*(\d+)\s*Rows_examined:\s*(\d+)`)

type MySQLSlowLogParser struct {
	results      []SlowResult
	result       *SlowResult
	query        strings.Builder
	mask         bool
	captureQuery bool
}

func NewMySQLSlowLogParser() *MySQLSlowLogParser {
	return &MySQLSlowLogParser{}
}

func (m *MySQLSlowLogParser) flush() {
	if m.result == nil {
		return
	}

	if m.captureQuery {
		rawQuery := strings.TrimSpace(m.query.String())
		query := strings.ReplaceAll(rawQuery, ";", "")

		if m.mask {
			m.result.Query = maskQuery(query)
		} else {
			m.result.Query = query
		}

		m.captureQuery = false
		m.query.Reset()
	}

	if m.result.QueryTime != 0 {
		m.results = append(m.results, *m.result)
	}

	m.result = nil
}

func (m *MySQLSlowLogParser) init() {
	m.results = nil
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

	sort.Slice(m.results, func(i, j int) bool {
		return m.results[i].QueryTime > m.results[j].QueryTime
	})

	return m.results, nil
}

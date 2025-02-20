package slow

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

var timeRegex = regexp.MustCompile(`(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)`)
var userHostRegex = regexp.MustCompile(`(\S+\[\S+\])\s*@\s*(\S+(\s*\[\d+\.\d+\.\d+\.\d+\])?)`)
var performanceRegex = regexp.MustCompile(`Query_time:\s*([\d.]+)\s*Lock_time:\s*([\d.]+)\s*Rows_sent:\s*(\d+)\s*Rows_examined:\s*(\d+)`)

type MySQLSlowLogParser struct {
	results   []SlowResult
	result    *SlowResult
	query     string
	saveQuery bool
}

func NewMySQLSlowLogParser() *MySQLSlowLogParser {
	return &MySQLSlowLogParser{}
}

func (m *MySQLSlowLogParser) flush() {
	if m.result != nil {
		if m.saveQuery {
			m.result.Query = strings.TrimSpace(m.query)
			m.resetQuery()
		}

		m.results = append(m.results, *m.result)
		m.result = nil
	}
}

func (m *MySQLSlowLogParser) resetQuery() {
	m.saveQuery = false
	m.query = ""
}

func (m *MySQLSlowLogParser) init() {
	m.results = nil
	m.result = nil
	m.query = ""
	m.saveQuery = false
}

func (m *MySQLSlowLogParser) setResult() {
	m.result = &SlowResult{}
}

func (m *MySQLSlowLogParser) parse(file io.Reader) ([]SlowResult, error) {
	m.init()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "# Time") {
			m.flush()
			m.setResult()

			matches := timeRegex.FindStringSubmatch(line)

			if len(matches) <= 1 {
				return nil, fmt.Errorf("fail to parse time")
			}

			m.result.Time = matches[1]
		}

		if m.saveQuery {
			m.query += line
		}

		if m.result != nil && strings.HasPrefix(line, "# User") {
			matches := userHostRegex.FindStringSubmatch(line)

			if len(matches) <= 2 {
				return nil, fmt.Errorf("fail to parse user and host")
			}

			userDb := matches[1]
			hostIp := matches[2]

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

			m.result.QueryTime = queryTime
			m.result.LockTime = lockTime
			m.result.RowsSent = uint(rowsSent)
			m.result.RowsExamined = uint(rowsExamined)
		}

		if m.result != nil && strings.HasPrefix(line, "SET") {
			m.saveQuery = true
		}
	}

	m.flush()

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return m.results, nil
}

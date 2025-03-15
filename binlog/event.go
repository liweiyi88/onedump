package binlog

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"
)

// https://github.com/mysql/mysql-server/blob/6b6d3ed3d5c6591b446276184642d7d0504ecc86/libs/mysql/binlog/event/binlog_event.cpp#L34
var mysqlChecksumVersionSplit = []int{5, 6, 1}
var mysqlChecksumVersionProduct = (mysqlChecksumVersionSplit[0]*256+mysqlChecksumVersionSplit[1])*256 + mysqlChecksumVersionSplit[2]

type eventType byte

type event interface {
	parse(h *eventHeader, body []byte) error
}

const (
	BINLOG_CHECKSUM_ALG_OFF   byte = 0
	BINLOG_CHECKSUM_ALG_CRC32 byte = 1   // CRC32
	BINLOG_CHECKSUM_ALG_UNDEF byte = 255 // undefined
)

// Event type and type code are defined here: https://github.com/mysql/mysql-server/blob/8.4/libs/mysql/binlog/event/binlog_event.h
const (
	UNKNOWN_EVENT eventType = iota
	START_EVENT_V3
	QUERY_EVENT
	STOP_EVENT
	ROTATE_EVENT
	INTVAR_EVENT
	LOAD_EVENT
	SLAVE_EVENT
	CREATE_FILE_EVENT
	APPEND_BLOCK_EVENT
	EXEC_LOAD_EVENT
	DELETE_FILE_EVENT
	NEW_LOAD_EVENT
	RAND_EVENT
	USER_VAR_EVENT
	FORMAT_DESCRIPTION_EVENT
	XID_EVENT
	BEGIN_LOAD_QUERY_EVENT
	EXECUTE_LOAD_QUERY_EVENT
	TABLE_MAP_EVENT
	WRITE_ROWS_EVENTv0
	UPDATE_ROWS_EVENTv0
	DELETE_ROWS_EVENTv0
	WRITE_ROWS_EVENTv1
	UPDATE_ROWS_EVENTv1
	DELETE_ROWS_EVENTv1
	INCIDENT_EVENT
	HEARTBEAT_EVENT
	IGNORABLE_EVENT
	ROWS_QUERY_EVENT
	WRITE_ROWS_EVENTv2
	UPDATE_ROWS_EVENTv2
	DELETE_ROWS_EVENTv2
	GTID_EVENT
	ANONYMOUS_GTID_EVENT
	PREVIOUS_GTIDS_EVENT
	TRANSACTION_CONTEXT_EVENT
	VIEW_CHANGE_EVENT
	XA_PREPARE_LOG_EVENT
	PARTIAL_UPDATE_ROWS_EVENT
	TRANSACTION_PAYLOAD_EVENT
	HEARTBEAT_LOG_EVENT_V2
	GTID_TAGGED_LOG_EVENT
)

type eventHeader struct {
	timestamp   uint32 // 4 bytes
	eventType   byte   // 1 byte
	serverId    uint32 // 4 bytes
	eventSize   uint32 // 4 bytes
	logPosition uint32 // 4 bytes
	flag        uint16 // 2 bytes
}

func parseEvent(h *eventHeader, body []byte) event {
	switch h.eventType {
	case byte(FORMAT_DESCRIPTION_EVENT):
		event := newFormatDescriptionEvent()
		event.parse(h, body)
		return event

	case byte(START_EVENT_V3):
		event := newStartEvetV3()
		event.parse(h, body)
		return event
	default:
		return nil
	}
}

func parseEventHeader(data []byte) (*eventHeader, error) {
	if len(data) != EventHeaderSize {
		return nil, fmt.Errorf("invalid binlog header length, expected: %d, got: %d", EventHeaderSize, len(data))
	}

	eventHeader := &eventHeader{}

	pos := 0
	eventHeader.timestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	eventHeader.eventType = data[pos]
	pos++

	eventHeader.serverId = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	eventHeader.eventSize = binary.LittleEndian.Uint32(data[pos : pos+4])

	pos += 4

	eventHeader.logPosition = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	eventHeader.flag = binary.LittleEndian.Uint16(data[pos:])

	return eventHeader, nil
}

// see https://github.com/mysql/mysql-server/blob/6b6d3ed3d5c6591b446276184642d7d0504ecc86/libs/mysql/binlog/event/binlog_event.cpp#L34
func calculateVersionProduct(version string) int {
	major, minor, patch := splitServerVersion(version)

	return (major*256+minor)*256 + patch
}

// It parses a MySQL version string (e.g., "8.0.34") into major, minor, and patch numbers.
// It is a rewrite of https://github.com/mysql/mysql-server/blob/6b6d3ed3d5c6591b446276184642d7d0504ecc86/libs/mysql/binlog/event/binlog_event.h#L184
func splitServerVersion(version string) (major, minor, patch int) {
	regex := regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)
	matches := regex.FindStringSubmatch(version)

	if len(matches) < 4 {
		return 0, 0, 0
	}

	var numbers [3]int
	for i := range 3 {
		num, err := strconv.Atoi(matches[i+1])
		if err != nil || num > 255 {
			return 0, 0, 0
		}
		numbers[i] = num
	}

	return numbers[0], numbers[1], numbers[2]
}

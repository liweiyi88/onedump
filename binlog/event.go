package binlog

import (
	"encoding/binary"
	"fmt"
	"io"
)

// https://github.com/mysql/mysql-server/blob/6b6d3ed3d5c6591b446276184642d7d0504ecc86/libs/mysql/binlog/event/binlog_event.cpp#L34
var mysqlChecksumVersionSplit = []int{5, 6, 1}
var mysqlChecksumVersionProduct = (mysqlChecksumVersionSplit[0]*256+mysqlChecksumVersionSplit[1])*256 + mysqlChecksumVersionSplit[2]

type eventType byte

type event interface {
	resolve(h *EventHeader, body []byte) error
	getHeader() *EventHeader
	print(io.Writer)
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

type EventHeader struct {
	Timestamp   uint32 `json:"timestamp"`    // 4 bytes
	EventType   byte   `json:"event_type"`   // 1 byte
	ServerId    uint32 `json:"server_id"`    // 4 bytes
	EventSize   uint32 `json:"event_size"`   // 4 bytes
	LogPosition uint32 `json:"log_position"` // 4 bytes
	Flag        uint16 `json:"flag"`         // 2 bytes
}

func parseEventHeader(data []byte) (*EventHeader, error) {
	if len(data) != EventHeaderSize {
		return nil, fmt.Errorf("invalid binlog header length, expected: %d, got: %d", EventHeaderSize, len(data))
	}

	eventHeader := &EventHeader{}

	pos := 0
	eventHeader.Timestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	eventHeader.EventType = data[pos]
	pos++

	eventHeader.ServerId = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	eventHeader.EventSize = binary.LittleEndian.Uint32(data[pos : pos+4])

	pos += 4

	eventHeader.LogPosition = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	eventHeader.Flag = binary.LittleEndian.Uint16(data[pos:])

	return eventHeader, nil
}

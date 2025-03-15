package binlog

import (
	"encoding/binary"
	"fmt"
)

type formatDescriptionEvent struct {
	header            *eventHeader
	code              eventType
	name              string
	serverVersion     string // 50 bytes
	binlogVersion     uint16 // 2 bytes
	createTimeStamp   uint32 // 4 bytes
	eventHeaderSize   uint8  // 1 byte
	postHeaderLengths []byte // around 40 bytes for MySQL 8+, each byte represents a post header length for that event type
	checksumAlgorithm byte   // 0 is off, 1 is for CRC32, 255 is undefined
	checksum          []byte // the last 4 bytes if it is enabled.
}

func newFormatDescriptionEvent() *formatDescriptionEvent {
	return &formatDescriptionEvent{
		code: FORMAT_DESCRIPTION_EVENT,
		name: "FORMAT_DESCRIPTION_EVENT",
	}
}

func (f *formatDescriptionEvent) parse(h *eventHeader, body []byte) error {
	size := uint32(EventHeaderSize + len(body))

	if h.eventSize != size {
		return fmt.Errorf("invalid start event size, expect size %d, received: %d", h.eventSize, size)
	}

	f.header = h

	pos := 0
	f.binlogVersion = binary.LittleEndian.Uint16(body[pos:2])
	pos += 2

	mysqlServerVersion := make([]byte, 50)
	copy(mysqlServerVersion, body[pos:])
	f.serverVersion = string(mysqlServerVersion)
	pos += 50

	f.createTimeStamp = binary.LittleEndian.Uint32(body[pos : pos+4])
	pos += 4

	f.eventHeaderSize = body[pos]

	if f.eventHeaderSize != byte(EventHeaderSize) {
		return fmt.Errorf("invalid event header length %d,  expect %d", f.eventHeaderSize, EventHeaderSize)
	}

	pos++

	// Checking if event has the checksum information.
	// see https://github.com/mysql/mysql-server/blob/8.4/libs/mysql/binlog/event/binlog_event.cpp#L147 for more details
	if calculateVersionProduct(f.serverVersion) < mysqlChecksumVersionProduct {
		f.postHeaderLengths = body[pos:]
		f.checksumAlgorithm = BINLOG_CHECKSUM_ALG_UNDEF
	} else {
		f.postHeaderLengths = body[pos : len(body)-5]
		f.checksumAlgorithm = body[len(body)-5] // the last 5th byte is the, the remaining 4 are the checksum
		f.checksum = body[len(body)-4:]
	}

	return nil
}

package binlog

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
)

type FormatDescriptionEvent struct {
	Header            *EventHeader `json:"header"`
	Code              eventType    `json:"type_code"`
	Name              string       `json:"name"`
	ServerVersion     string       `json:"server_version"`    // 50 bytes
	BinlogVersion     uint16       `json:"binlog_version"`    // 2 bytes
	CreateTimeStamp   uint32       `json:"create_timestamp"`  // 4 bytes
	EventHeaderSize   uint8        `json:"event_header_size"` // 1 byte
	PostHeaderLengths []byte       // around 40 bytes for MySQL 8+, each byte represents a post header length for that event type
	ChecksumAlgorithm byte         `json:"checksum_algorithm"` // 0 is off, 1 is for CRC32, 255 is undefined
	Checksum          []byte       // the last 4 bytes if it is enabled.
}

func newFormatDescriptionEvent() *FormatDescriptionEvent {
	return &FormatDescriptionEvent{
		Code: FORMAT_DESCRIPTION_EVENT,
		Name: "FORMAT_DESCRIPTION_EVENT",
	}
}

func (f *FormatDescriptionEvent) getHeader() *EventHeader {
	return f.Header
}

func (f *FormatDescriptionEvent) print(w io.Writer) {
	fmt.Fprintf(w, "=== %s ===\n", f.Name)
	fmt.Fprintf(w, "Event type: %d\n", f.Code)
	fmt.Fprintf(w, "Server version: %s\n", f.ServerVersion)
	fmt.Fprintf(w, "Binlog version: %d\n", f.BinlogVersion)
	fmt.Fprintf(w, "Checksum algorithm: %d\n", f.ChecksumAlgorithm)
	fmt.Fprintf(w, "Post-header lengths: %s", hex.Dump(f.PostHeaderLengths))
	fmt.Fprintln(w)
}

func (f *FormatDescriptionEvent) resolve(h *EventHeader, body []byte) error {
	size := uint32(EventHeaderSize + len(body))

	if h.EventSize != size {
		return fmt.Errorf("invalid start event size, expect size %d, received: %d", h.EventSize, size)
	}

	f.Header = h

	pos := 0
	f.BinlogVersion = binary.LittleEndian.Uint16(body[pos:2])
	pos += 2

	mysqlServerVersion := make([]byte, 50)
	copy(mysqlServerVersion, body[pos:])
	f.ServerVersion = string(mysqlServerVersion)
	pos += 50

	f.CreateTimeStamp = binary.LittleEndian.Uint32(body[pos : pos+4])
	pos += 4

	f.EventHeaderSize = body[pos]

	if f.EventHeaderSize != byte(EventHeaderSize) {
		return fmt.Errorf("invalid event header length %d,  expect %d", f.EventHeaderSize, EventHeaderSize)
	}

	pos++

	// Checking if event has the checksum information.
	// see https://github.com/mysql/mysql-server/blob/8.4/libs/mysql/binlog/event/binlog_event.cpp#L147 for more details
	if calculateVersionProduct(f.ServerVersion) < mysqlChecksumVersionProduct {
		f.PostHeaderLengths = body[pos:]
		f.ChecksumAlgorithm = BINLOG_CHECKSUM_ALG_UNDEF
	} else {
		f.PostHeaderLengths = body[pos : len(body)-5]
		f.ChecksumAlgorithm = body[len(body)-5] // the last 5th byte is the, the remaining 4 are the checksum
		f.Checksum = body[len(body)-4:]
	}

	return nil
}

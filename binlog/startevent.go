package binlog

import (
	"encoding/binary"
	"fmt"
)

type startEventV3 struct {
	header             *eventHeader
	code               eventType
	name               string
	version            uint16 // the version of the start event
	mysqlServerVersion string // 50 bytes
	binlogVersion      uint16
	createTimeStamp    uint32
}

func newStartEvetV3() *startEventV3 {
	return &startEventV3{
		version: 3,
		code:    START_EVENT_V3,
		name:    "START_EVENT_V3",
	}
}

func (s *startEventV3) parse(h *eventHeader, body []byte) error {
	size := uint32(EventHeaderSize + len(body))

	if h.eventSize != size {
		return fmt.Errorf("invalid start event size, expect size %d, received: %d", h.eventSize, size)
	}

	pos := 0
	binlogVersion := binary.LittleEndian.Uint16(body[pos:2])
	pos += 2

	mysqlServerVersion := make([]byte, 50)
	copy(mysqlServerVersion, body[pos:pos+50])
	pos += 50

	createTimeStamp := binary.LittleEndian.Uint32(body[pos:])
	s.header = h
	s.binlogVersion = binlogVersion
	s.mysqlServerVersion = string(mysqlServerVersion)
	s.createTimeStamp = createTimeStamp

	return nil
}

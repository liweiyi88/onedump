package binlog

import (
	"encoding/binary"
	"fmt"
	"io"
)

type StartEventV3 struct {
	Header          *EventHeader `json:"header"`
	Code            eventType    `json:"type_code"`
	Name            string       `json:"name"`
	ServerVersion   string       `json:"server_version"` // 50 bytes
	BinlogVersion   uint16       `json:"binlog_version"`
	CreateTimeStamp uint32       `json:"create_timestamp"`
}

func newStartEvetV3() *StartEventV3 {
	return &StartEventV3{
		Code: START_EVENT_V3,
		Name: "START_EVENT_V3",
	}
}

func (s *StartEventV3) getHeader() *EventHeader {
	return s.Header
}

func (s *StartEventV3) print(w io.Writer) {
	fmt.Fprintf(w, "=== %s ===\n", s.Name)
	fmt.Fprintf(w, "Event type: %d\n", s.Code)
	fmt.Fprintf(w, "Server version: %s\n", s.ServerVersion)
	fmt.Fprintf(w, "Binlog version: %d\n", s.BinlogVersion)
	fmt.Fprintln(w)
}

func (s *StartEventV3) resolve(h *EventHeader, body []byte) error {
	size := uint32(EventHeaderSize + len(body))

	if h.EventSize != size {
		return fmt.Errorf("invalid start event size, expect size %d, received: %d", h.EventSize, size)
	}

	pos := 0
	binlogVersion := binary.LittleEndian.Uint16(body[pos:2])
	pos += 2

	mysqlServerVersion := make([]byte, 50)
	copy(mysqlServerVersion, body[pos:pos+50])
	pos += 50

	createTimeStamp := binary.LittleEndian.Uint32(body[pos:])
	s.Header = h
	s.BinlogVersion = binlogVersion
	s.ServerVersion = string(mysqlServerVersion)
	s.CreateTimeStamp = createTimeStamp

	return nil
}

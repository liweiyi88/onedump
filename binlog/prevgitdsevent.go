package binlog

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

type PreviousGTIDsEvent struct {
	Name     string
	Code     eventType
	Header   *EventHeader
	GTIDSets string
}

func newPreviousGTIDsEvent() *PreviousGTIDsEvent {
	return &PreviousGTIDsEvent{
		Name: "PREVIOUS_GTIDS_EVENT",
		Code: PREVIOUS_GTIDS_EVENT,
	}
}

func decodeSidFormat(data []byte) (format int, sidNumber uint64) {
	if data[7] == 1 {
		format = GtidFormatTagged
	}

	if format == GtidFormatTagged {
		masked := make([]byte, 8)
		copy(masked, data[1:7])
		sidNumber = binary.LittleEndian.Uint64(masked)
		return
	}

	sidNumber = binary.LittleEndian.Uint64(data[:8])
	return
}

func bytesToUUID(data []byte) string {
	hexStr := hex.EncodeToString(data)
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hexStr[0:8], hexStr[8:12], hexStr[12:16], hexStr[16:20], hexStr[20:32])
}

// TODO: read https://github.com/mysql/mysql-server/blob/8.4/sql/log_event.cc#L13546
// gtid encode, decode https://github.com/mysql/mysql-server/blob/trunk/sql/rpl_gtid.h#L2247
func (p *PreviousGTIDsEvent) resolve(h *EventHeader, body []byte) error {
	p.Header = h

	emptyBytes := make([]byte, len(body))
	if bytes.Equal(emptyBytes, body) {
		// empty GTID set
		return nil
	}

	pos := 0
	format, sidNumber := decodeSidFormat(body)
	pos += 8

	var sb strings.Builder
	for i := range sidNumber {
		uuid := bytesToUUID(body[pos : pos+16])
		pos += 16

		var tag string
		if format == GtidFormatTagged {
			tagLength := int(body[pos]) / 2
			pos += 1
			if tagLength > 0 {
				tag = string(body[pos : pos+tagLength])
				pos += tagLength
			}
		}

		if len(tag) > 0 {
			sb.WriteString(":")
			sb.WriteString(tag)
		} else {
			if i != 0 {
				sb.WriteString(",")
			}

			sb.WriteString(uuid)
		}

		sliceCount := binary.LittleEndian.Uint16(body[pos : pos+8])
		pos += 8
		for range sliceCount {
			sb.WriteString(":")

			start := binary.LittleEndian.Uint64(body[pos : pos+8])
			pos += 8
			stop := binary.LittleEndian.Uint64(body[pos : pos+8])
			pos += 8
			if stop == start+1 {
				fmt.Fprintf(&sb, "%d", start)
			} else {
				fmt.Fprintf(&sb, "%d-%d", start, stop-1)
			}
		}
	}

	p.GTIDSets = sb.String()
	return nil
}

func (p *PreviousGTIDsEvent) getHeader() *EventHeader {
	return p.Header
}

func (p *PreviousGTIDsEvent) print(w io.Writer) {
	fmt.Fprintf(w, "=== %s ===\n", p.Name)
	fmt.Fprintf(w, "Event type: %d\n", p.Code)
	fmt.Fprintf(w, "GTID Sets: %s\n", p.GTIDSets)
	fmt.Fprintln(w)
}

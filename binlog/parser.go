package binlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
)

const (
	MagicNumberLen  = 4
	EventHeaderSize = 19
	CheckSumBytes   = 4
)

// The MySQL binlog magic prefix
var binlogMagicPrefix = [4]byte{0xFE, 0x62, 0x69, 0x6E}

type BinlogParser struct {
	fde *FormatDescriptionEvent
}

func NewBinlogParser() *BinlogParser {
	return &BinlogParser{}
}

func (b *BinlogParser) getEventDataSize(eventSize uint32) uint32 {
	if b.fde != nil &&
		b.fde.ChecksumAlgorithm != BINLOG_CHECKSUM_ALG_OFF &&
		b.fde.ChecksumAlgorithm != BINLOG_CHECKSUM_ALG_UNDEF {
		return eventSize - EventHeaderSize - CheckSumBytes
	}

	return eventSize - EventHeaderSize
}

func (b *BinlogParser) parseEvent(h *EventHeader, data []byte) (event, error) {
	if h == nil {
		return nil, errors.New("event header is nil")
	}

	// TODO: verify checksum when checksum is enabled

	var e event

	switch h.EventType {
	case byte(FORMAT_DESCRIPTION_EVENT):
		e = newFormatDescriptionEvent()
	case byte(PREVIOUS_GTIDS_EVENT):
		e = newPreviousGTIDsEvent()
	case byte(START_EVENT_V3):
		e = newStartEvetV3()
	default:
		e = newDefaultEvent()
	}

	err := e.resolve(h, data[:b.getEventDataSize(h.EventSize)])
	return e, err
}

func (b *BinlogParser) ParseFile(filename string) error {
	file, err := os.Open(filename)

	defer func() {
		err := file.Close()
		if err != nil {
			slog.Error("fail to close file", slog.Any("error", err))
		}
	}()

	if err != nil {
		return fmt.Errorf("fail to open file: %s, error: %v", filename, err)
	}

	// Checking the magic prefix
	magicNumber := make([]byte, MagicNumberLen)
	_, err = file.Read(magicNumber)

	if err != nil {
		return err
	}

	if !bytes.Equal(binlogMagicPrefix[:], magicNumber) {
		return errors.New("invalid binlog")
	}

	for {
		// Read event header
		headerBytes := make([]byte, EventHeaderSize)
		_, err = file.Read(headerBytes)

		if err != nil {
			if err == io.EOF {
				return nil
			}
			// checking if err is io.EOF
			return fmt.Errorf("fail to read binlog header, error: %v", err)
		}

		eventHeader, err := parseEventHeader(headerBytes)
		if err != nil {
			return err
		}

		bodySize := eventHeader.EventSize - EventHeaderSize

		body := make([]byte, bodySize)
		_, err = file.Read(body)

		if err != nil {
			return err
		}

		event, err := b.parseEvent(eventHeader, body)
		if err != nil {
			return err
		}

		fde, ok := event.(*FormatDescriptionEvent)
		if ok {
			b.fde = fde
		}

		if event != nil {
			event.print(os.Stdout)
		}
	}
}

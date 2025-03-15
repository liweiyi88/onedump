package binlog

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

const (
	MagicNumberLen  = 4
	EventHeaderSize = 19
)

// The MySQL binlog magic prefix
var binlogMagicPrefix = [4]byte{0xFE, 0x62, 0x69, 0x6E}

func ParseFile(filename string) error {
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

	// Read event header
	headerBytes := make([]byte, EventHeaderSize)
	_, err = file.Read(headerBytes)

	if err != nil {
		// checking if err is io.EOF
		return fmt.Errorf("fail to read binlog header, error: %v", err)
	}

	eventHeader, err := parseEventHeader(headerBytes)
	if err != nil {
		return err
	}

	bodySize := eventHeader.eventSize - EventHeaderSize

	body := make([]byte, bodySize)
	_, err = file.Read(body)

	if err != nil {
		return err
	}

	e := parseEvent(eventHeader, body)

	fmt.Printf("%+v", e)

	return nil
}

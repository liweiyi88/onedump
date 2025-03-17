package binlog

import (
	"fmt"
	"io"
)

type DefaultEvent struct {
	Name   string       `json:"name"`
	Code   eventType    `json:"type_code"`
	Header *EventHeader `json:"header"`
	Body   []byte
}

func newDefaultEvent() *DefaultEvent {
	return &DefaultEvent{
		Name: "DEFAULT_EVENT",
	}
}

func (d *DefaultEvent) print(w io.Writer) {
	fmt.Fprintf(w, "=== %s ===\n", d.Name)
	fmt.Fprintf(w, "Event type: %d\n", d.Code)
	fmt.Fprintln(w)
}

func (d *DefaultEvent) resolve(h *EventHeader, body []byte) error {
	d.Header = h
	d.Code = eventType(h.EventType)
	d.Body = body
	return nil
}

func (d *DefaultEvent) getHeader() *EventHeader {
	return d.Header
}

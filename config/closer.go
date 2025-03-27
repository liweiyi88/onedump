package config

import (
	"errors"
	"io"
)

type MultiCloser struct {
	closers []io.Closer
}

func NewMultiCloser(closers []io.Closer) *MultiCloser {
	return &MultiCloser{
		closers: closers,
	}
}

func (m *MultiCloser) Close() error {
	var errs error
	for _, c := range m.closers {
		if err := c.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

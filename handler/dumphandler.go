package handler

import (
	"io"
	"reflect"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/notifier/console"
)

type Notifier interface {
	Notify(message []string) error
}

type Storage interface {
	Save(reader io.Reader, gzip bool, unique bool) error
}

type DumpHandler struct {
	Dump *config.Dump
}

func NewDumpHandler(dump *config.Dump) *DumpHandler {
	return &DumpHandler{
		Dump: dump,
	}
}

func (d *DumpHandler) Do() error {
	var dumpErr error
	var wg sync.WaitGroup

	messages := make([]string, 0, len(d.Dump.Jobs))

	for _, job := range d.Dump.Jobs {
		wg.Add(1)
		go func(job *config.Job) {
			jobHandler := NewJobHandler(job)
			result := jobHandler.Do()
			if result.Error != nil {
				dumpErr = multierror.Append(dumpErr, result.Error)
			}

			messages = append(messages, result.String())
			wg.Done()
		}(job)
	}

	wg.Wait()

	err := d.notify(messages)
	if err != nil {
		dumpErr = multierror.Append(dumpErr, err)
	}

	return dumpErr
}

func (d *DumpHandler) notify(message []string) error {
	var err error
	var wg sync.WaitGroup
	for _, notifier := range d.getNotifiers() {
		wg.Add(1)
		go func(notifier Notifier) {
			err := notifier.Notify(message)
			if err != nil {
				err = multierror.Append(err, err)
			}
			wg.Done()
		}(notifier)
	}

	wg.Wait()
	return err
}

func (d *DumpHandler) getNotifiers() []Notifier {
	var notifiers []Notifier
	notifiers = append(notifiers, console.New())

	v := reflect.ValueOf(d.Dump.Notifier)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		switch field.Kind() {
		case reflect.Slice:
			for i := 0; i < field.Len(); i++ {
				n, ok := field.Index(i).Interface().(Notifier)
				if ok {
					notifiers = append(notifiers, n)
				}
			}
		}
	}

	return notifiers
}

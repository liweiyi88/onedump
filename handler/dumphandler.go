package handler

import (
	"io"
	"reflect"
	"sync"

	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/jobresult"
	"github.com/liweiyi88/onedump/notifier/console"

	"github.com/hashicorp/go-multierror"
)

type Notifier interface {
	Notify(results []*jobresult.JobResult) error
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

	results := make([]*jobresult.JobResult, 0, len(d.Dump.Jobs))

	for _, job := range d.Dump.Jobs {
		wg.Add(1)
		go func(job *config.Job) {
			jobHandler := NewJobHandler(job)
			result := jobHandler.Do()
			if result.Error != nil {
				dumpErr = multierror.Append(dumpErr, result.Error)
			}

			results = append(results, result)
			wg.Done()
		}(job)
	}

	wg.Wait()

	err := d.notify(results)

	if err != nil {
		dumpErr = multierror.Append(dumpErr, err)
	}

	return dumpErr
}

func (d *DumpHandler) notify(results []*jobresult.JobResult) error {
	var err error
	var wg sync.WaitGroup
	for _, notifier := range d.getNotifiers() {
		wg.Add(1)
		go func(notifier Notifier) {
			notifErr := notifier.Notify(results)
			if notifErr != nil {
				err = multierror.Append(err, notifErr)
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

package handler

import (
	"errors"
	"reflect"
	"sync"

	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/jobresult"
	"github.com/liweiyi88/onedump/notifier/console"
)

type Notifier interface {
	Notify(results []*jobresult.JobResult) error
}

type DumpHandler struct {
	Dump *config.Dump
	mu   sync.Mutex
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
			defer wg.Done()

			jobHandler := NewJobHandler(job)
			result := jobHandler.Do()

			d.mu.Lock()
			if result.Error != nil {
				dumpErr = errors.Join(dumpErr, result.Error)
			}

			results = append(results, result)
			d.mu.Unlock()

		}(job)
	}

	wg.Wait()

	err := d.notify(results)

	if err != nil {
		dumpErr = errors.Join(dumpErr, err)
	}

	return dumpErr
}

func (d *DumpHandler) notify(results []*jobresult.JobResult) error {
	var errs error
	var wg sync.WaitGroup
	for _, notifier := range d.getNotifiers() {
		wg.Add(1)
		go func(notifier Notifier) {
			notifErr := notifier.Notify(results)
			if notifErr != nil {
				d.mu.Lock()
				errs = errors.Join(errs, notifErr)
				d.mu.Unlock()
			}
			wg.Done()
		}(notifier)
	}

	wg.Wait()
	return errs
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

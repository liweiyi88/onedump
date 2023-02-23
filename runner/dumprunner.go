package runner

import (
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/liweiyi88/onedump/config"
)

type DumpRunner struct {
	Dump *config.Dump
}

func NewDumpRunner(dump *config.Dump) *DumpRunner {
	return &DumpRunner{
		Dump: dump,
	}
}

func (d *DumpRunner) Do() error {
	var dumpErr error
	var wg sync.WaitGroup

	messages := make([]string, 0, len(d.Dump.Jobs))

	for _, job := range d.Dump.Jobs {
		wg.Add(1)
		go func(job *config.Job) {
			jobRunner := NewJobRunner(job)
			result := jobRunner.Run()
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

func (d *DumpRunner) notify(message []string) error {
	var err error
	var wg sync.WaitGroup
	for _, notifier := range d.Dump.GetNotifiers() {
		wg.Add(1)
		go func(notifier config.Notifier) {
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

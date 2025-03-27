package handler

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/dumper"
	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/jobresult"
	"github.com/liweiyi88/onedump/storage"
)

type JobHandler struct {
	Job *config.Job
}

// Create a new job handler.
func NewJobHandler(job *config.Job) *JobHandler {
	return &JobHandler{
		Job: job,
	}
}

// Pipe readers, writer and closers for fanout the same writer.
func storageReadWriteCloser(count int, compress bool) ([]io.Reader, io.Writer, io.Closer) {
	var prs []io.Reader
	var pws []io.Writer
	var pcs []io.Closer
	for i := 0; i < count; i++ {
		pr, pw := io.Pipe()

		prs = append(prs, pr)

		if compress {
			gw := gzip.NewWriter(pw)
			pws = append(pws, gw)
			pcs = append(pcs, gw)
		} else {
			pws = append(pws, pw)
		}

		// This following append method must not be moved before pcs = append(pcs, gw) if compress is in use as the closer won't be able to close properly.
		// Thus, we put this line here and do not move it to other place.
		pcs = append(pcs, pw)
	}

	return prs, io.MultiWriter(pws...), config.NewMultiCloser(pcs)
}

// Save database dump to different storages.
func (handler *JobHandler) save() error {
	job := handler.Job
	storages := handler.getStorages()

	numberOfStorages := len(storages)

	errCh := make(chan error, numberOfStorages+1)

	dumper, err := handler.getDumper()

	if err != nil {
		return fmt.Errorf("could not get dumper: %v", err)
	}

	if numberOfStorages > 0 {
		// Use pipe to pass content from the database dump to different writer.
		readers, writer, closer := storageReadWriteCloser(numberOfStorages, job.Gzip)

		var dumpWg sync.WaitGroup
		dumpWg.Add(1)
		go func() {
			err := dumper.Dump(writer)
			if err != nil {
				errCh <- err
			}

			// We must call .Done before the closer.Close method
			// writer and readers are connected via pipe and readers wait for the closer.Close to signal EOF so they can finish reading.
			// If we call .Done after close then it will block as dumpWg has not finished yet while readers wait for the EOF signal.
			dumpWg.Done()

			// We must call closer.Close() after the dump call. Then it will signal all readers with proper EOF.
			if closeErr := closer.Close(); closeErr != nil {
				slog.Error("can not close pipe readers and writers", slog.Any("error", closeErr))
			}
		}()

		var readWg sync.WaitGroup
		readWg.Add(numberOfStorages)
		for i, s := range storages {
			storage := s
			go func(i int) {
				defer readWg.Done()

				pathGenerator := func(filename string) string {
					return fileutil.EnsureFileName(filename, job.Gzip, job.Unique)
				}

				e := storage.Save(readers[i], pathGenerator)
				if e != nil {
					errCh <- e
				}
			}(i)
		}

		go func() {
			dumpWg.Wait()
			readWg.Wait()
			close(errCh)
		}()

		var allErrors []error

		for err := range errCh {
			allErrors = append(allErrors, err)
		}

		if len(allErrors) > 0 {
			return errors.Join(allErrors...)
		}
	}

	return nil
}

// Get all storage structs based on job configuration.
func (handler *JobHandler) getStorages() []storage.Storage {
	var storages []storage.Storage

	v := reflect.ValueOf(handler.Job.Storage)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		switch field.Kind() {
		case reflect.Slice:
			for i := 0; i < field.Len(); i++ {
				s, ok := field.Index(i).Interface().(storage.Storage)
				if ok {
					storages = append(storages, s)
				}
			}
		}
	}

	return storages
}

// Get the database dumper.
func (handler *JobHandler) getDumper() (dumper.Dumper, error) {
	job := handler.Job

	switch job.DBDriver {
	case "mysql":
		return dumper.NewMysqlNativeDump(job)
	case "postgresql":
		return dumper.NewPgDump(job)
	case "mysqldump":
		return dumper.NewMysqlDump(job)
	case "pgdump":
		return dumper.NewPgDump(job)
	default:
		return nil, fmt.Errorf("%s is not a supported database driver", job.DBDriver)
	}
}

// Do the job.
func (handler *JobHandler) Do() *jobresult.JobResult {
	start := time.Now()
	result := &jobresult.JobResult{}

	defer func() {
		elapsed := time.Since(start)
		result.Elapsed = elapsed
	}()

	result.JobName = handler.Job.Name

	err := handler.save()
	if err != nil {
		result.Error = fmt.Errorf("failed to store dump file %v", err)
	}

	return result
}

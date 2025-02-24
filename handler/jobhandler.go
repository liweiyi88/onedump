package handler

import (
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
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
	var err error

	dumper, err := handler.getDumper()

	if err != nil {
		return fmt.Errorf("could not get dumper: %v", err)
	}

	job := handler.Job
	storages := handler.getStorages()
	numberOfStorages := len(storages)

	if numberOfStorages > 0 {
		// Use pipe to pass content from the database dump to different writer.
		readers, writer, closer := storageReadWriteCloser(numberOfStorages, job.Gzip)

		go func() {
			e := dumper.Dump(writer)
			if e != nil {
				err = multierror.Append(err, e)
			}

			closeErr := closer.Close()
			if closeErr != nil {
				slog.Error("can not close pipe readers and writers", slog.Any("error", closeErr))
			}
		}()

		var wg sync.WaitGroup
		wg.Add(numberOfStorages)
		for i, s := range storages {
			storage := s
			go func(i int) {
				defer wg.Done()

				pathGenerator := func(filename string) string {
					return fileutil.EnsureFileName(filename, job.Gzip, job.Unique)
				}

				e := storage.Save(readers[i], pathGenerator)
				if e != nil {
					err = multierror.Append(err, e)
				}
			}(i)
		}

		wg.Wait()
	}

	return err
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

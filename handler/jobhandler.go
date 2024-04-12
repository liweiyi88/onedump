package handler

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/driver"
	"github.com/liweiyi88/onedump/dumper"
	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/jobresult"
	"github.com/liweiyi88/onedump/storage"
)

type JobHandler struct {
	Job *config.Job
}

func NewJobHandler(job *config.Job) *JobHandler {
	return &JobHandler{
		Job: job,
	}
}

// Pipe readers, writers and closer for fanout the same os.file
func storageReadWriteCloser(count int, compress bool) ([]io.Reader, io.Writer, io.Closer) {
	var prs []io.Reader
	var pws []io.Writer
	var pcs []io.Closer
	for i := 0; i < count; i++ {
		pr, pw := io.Pipe()

		if compress {
			gw := gzip.NewWriter(pw)

			prs = append(prs, pr)
			pws = append(pws, gw)
			pcs = append(pcs, gw)
			pcs = append(pcs, pw)
		} else {
			prs = append(prs, pr)
			pws = append(pws, pw)
			pcs = append(pcs, pw)
		}
	}

	return prs, io.MultiWriter(pws...), config.NewMultiCloser(pcs)
}

func (handler *JobHandler) save(dumper dumper.Dumper) error {
	job := handler.Job
	storages := handler.getStorages()
	numberOfStorages := len(storages)

	var err error

	if numberOfStorages > 0 {
		// Use pipe to pass content from the cache file to different writer.
		readers, writer, closer := storageReadWriteCloser(numberOfStorages, job.Gzip)

		go func() {
			e := dumper.Dump(writer)
			if e != nil {
				err = multierror.Append(err, e)
			}

			closeErr := closer.Close()
			if closeErr != nil {
				log.Printf("Cannot close pipe readers and writers: %v", closeErr)
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

func (handler *JobHandler) getDumper() (dumper.Dumper, error) {
	job := handler.Job
	driver, err := handler.getDBDriver()
	if err != nil {
		return nil, fmt.Errorf("failed to get db driver %v", err)
	}

	if job.ViaSsh() {
		return dumper.NewSshDumper(job.SshHost, job.SshKey, job.SshUser, driver), nil
	} else {
		return dumper.NewExecDumper(driver), nil
	}
}

func (handler *JobHandler) getDBDriver() (driver.Driver, error) {
	job := handler.Job
	switch job.DBDriver {
	case "mysql":
		driver, err := driver.NewMysqlDriver(job.DBDsn, job.DumpOptions, job.ViaSsh())
		if err != nil {
			return nil, err
		}

		return driver, nil
	case "postgresql":
		driver, err := driver.NewPostgreSqlDriver(job.DBDsn, job.DumpOptions, job.ViaSsh())
		if err != nil {
			return nil, err
		}

		return driver, nil
	default:
		return nil, fmt.Errorf("%s is not a supported database driver", job.DBDriver)
	}
}

func (handler *JobHandler) Do() *jobresult.JobResult {
	start := time.Now()
	result := &jobresult.JobResult{}

	defer func() {
		elapsed := time.Since(start)
		result.Elapsed = elapsed
	}()

	result.JobName = handler.Job.Name

	dumper, err := handler.getDumper()
	if err != nil {
		result.Error = fmt.Errorf("could not get dumper: %v", err)
		return result
	}

	err = handler.save(dumper)
	if err != nil {
		result.Error = fmt.Errorf("failed to store dump file %v", err)
	}

	return result
}

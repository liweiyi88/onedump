package handler

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/driver"
	"github.com/liweiyi88/onedump/dumper"
	"github.com/liweiyi88/onedump/fileutil"
	"github.com/liweiyi88/onedump/jobresult"
)

const cacheDirPrefix = ".onedump"

func init() {
	rand.Seed(time.Now().UnixNano())
}

type JobHandler struct {
	Job *config.Job
}

func NewJobHandler(job *config.Job) *JobHandler {
	return &JobHandler{
		Job: job,
	}
}

// For uploading dump file to remote storage, we need to firstly dump the db content to a dir locally.
// We firstly try to get current work dir, if not successful, then try to get home dir and finally try temp dir.
// Be aware of the size limit of a temp dir in different OS.
func cacheFileDir() string {
	// randomise the upload cache dir, otherwise we will have race condition when have more than one dump jobs.
	return fmt.Sprintf("%s/%s%s", fileutil.WorkDir(), cacheDirPrefix, fileutil.GenerateRandomName(4))
}

// Get the cache file path that stores the dump contents.
func cacheFilePath(cacheDir string, shouldGzip bool) string {
	filename := fmt.Sprintf("%s/%s", cacheDir, fileutil.GenerateRandomName(8)+".sql")
	return fileutil.EnsureFileSuffix(filename, shouldGzip)
}

// Create the cache file that stores the dump contents.
func createCacheFile(gzip bool) (*os.File, string, error) {
	cacheDir := cacheFileDir()
	err := os.MkdirAll(cacheDir, 0750)

	if err != nil {
		return nil, "", fmt.Errorf("failed to create cache dir for remote upload. %w", err)
	}

	dumpFileName := cacheFilePath(cacheDir, gzip)

	file, err := os.Create(dumpFileName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create cache file: %w", err)
	}

	return file, cacheDir, nil
}

// Pipe readers, writers and closer for fanout the same os.file
func storageReadWriteCloser(count int) ([]io.Reader, io.Writer, io.Closer) {
	var prs []io.Reader
	var pws []io.Writer
	var pcs []io.Closer
	for i := 0; i < count; i++ {
		pr, pw := io.Pipe()

		prs = append(prs, pr)
		pws = append(pws, pw)
		pcs = append(pcs, pw)
	}

	return prs, io.MultiWriter(pws...), config.NewMultiCloser(pcs)
}

// Dump the db to the cache file.
func (handler *JobHandler) dumpToCacheFile(dumper dumper.Dumper) (string, string, error) {
	file, cacheDir, err := createCacheFile(handler.Job.Gzip)

	if err != nil {
		return "", cacheDir, err
	}

	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close cache file: %v", err)
		}
	}()

	err = dumper.DumpToFile(file)
	if err != nil {
		return "", cacheDir, fmt.Errorf("failed to dump file: %v", err)
	}

	return file.Name(), cacheDir, nil
}

func (handler *JobHandler) save(cacheFile io.Reader) error {
	job := handler.Job
	storages := handler.getStorages()
	numberOfStorages := len(storages)

	var err error

	if numberOfStorages > 0 {
		// Use pipe to pass content from the cache file to different writer.
		readers, writer, closer := storageReadWriteCloser(numberOfStorages)

		go func() {
			_, e := io.Copy(writer, cacheFile)
			if e != nil {
				multierror.Append(err, e)
			}
			closer.Close()
		}()

		var wg sync.WaitGroup
		wg.Add(numberOfStorages)
		for i, s := range storages {
			storage := s
			go func(i int) {
				defer wg.Done()
				e := storage.Save(readers[i], job.Gzip, job.Unique)
				if e != nil {
					err = multierror.Append(err, e)
				}
			}(i)
		}

		wg.Wait()
	}

	return err
}

func (handler *JobHandler) getStorages() []Storage {
	var storages []Storage

	v := reflect.ValueOf(handler.Job.Storage)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		switch field.Kind() {
		case reflect.Slice:
			for i := 0; i < field.Len(); i++ {
				s, ok := field.Index(i).Interface().(Storage)
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
		return dumper.NewSshDumper(job.SshHost, job.SshKey, job.SshUser, job.Gzip, driver), nil
	} else {
		return dumper.NewExecDumper(job.Gzip, driver), nil
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

	cacheFileName, cacheDir, err := handler.dumpToCacheFile(dumper)
	defer func() {
		err := os.RemoveAll(cacheDir)
		if err != nil {
			log.Println("failed to remove cache dir after dump", err)
		}
	}()

	if err != nil {
		result.Error = fmt.Errorf("failed to dump content to cache file: %v", err)
		return result
	}

	cacheFile, err := os.Open(cacheFileName)
	if err != nil {
		result.Error = fmt.Errorf("failed to open cache file: %v", err)
		return result
	}

	defer func() {
		err := cacheFile.Close()
		if err != nil {
			log.Printf("failed to close cache file: %v", err)
		}
	}()

	err = handler.save(cacheFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to store dump file %v", err)
	}

	return result
}

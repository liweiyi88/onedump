package runner

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/liweiyi88/onedump/config"
	"github.com/liweiyi88/onedump/dumper"
	"github.com/liweiyi88/onedump/fileutil"
)

const cacheDirPrefix = ".onedump"

func init() {
	rand.Seed(time.Now().UnixNano())
}

type JobRunner struct {
	Job *config.Job
}

func NewJobRunner(job *config.Job) *JobRunner {
	return &JobRunner{
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

// Dump the db to the cache file.
func (dumper *JobRunner) dumpToCacheFile(runner dumper.Dumper) (string, string, error) {
	file, cacheDir, err := createCacheFile(dumper.Job.Gzip)

	if err != nil {
		return "", cacheDir, err
	}

	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close cache file: %v", err)
		}
	}()

	err = runner.DumpToFile(file)
	if err != nil {
		return "", cacheDir, fmt.Errorf("failed to dump file: %v", err)
	}

	return file.Name(), cacheDir, nil
}

func (jobRunner *JobRunner) save(cacheFile io.Reader) error {
	job := jobRunner.Job
	storages := job.GetStorages()
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

func (jobRunner *JobRunner) Run() *config.JobResult {
	start := time.Now()
	result := &config.JobResult{}

	defer func() {
		elapsed := time.Since(start)
		result.Elapsed = elapsed
	}()

	result.JobName = jobRunner.Job.Name

	runner, err := jobRunner.Job.GetRunner()
	if err != nil {
		result.Error = fmt.Errorf("could not get job runner: %v", err)
		return result
	}

	cacheFileName, cacheDir, err := jobRunner.dumpToCacheFile(runner)
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

	err = jobRunner.save(cacheFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to store dump file %v", err)
	}

	return result
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

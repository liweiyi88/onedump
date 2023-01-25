package dumper

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
	"github.com/liweiyi88/onedump/filenaming"
)

const cacheDirPrefix = ".onedump"

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Dumper struct {
	Job *config.Job
}

func NewDumper(job *config.Job) *Dumper {
	return &Dumper{
		Job: job,
	}
}

func generateRandomName(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// For uploading dump file to remote storage, we need to firstly dump the db content to a dir locally.
// We firstly try to get current work dir, if not successful, then try to get home dir and finally try temp dir.
// Be aware of the size limit of a temp dir in different OS.
func cacheFileDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Printf("Cannot get the current directory: %v, using $HOME directory!", err)
		dir, err = os.UserHomeDir()
		if err != nil {
			log.Printf("Cannot get the user home directory: %v, using /tmp directory!", err)
			dir = os.TempDir()
		}
	}

	// randomise the upload cache dir, otherwise we will have race condition when have more than one dump jobs.
	return fmt.Sprintf("%s/%s%s", dir, cacheDirPrefix, generateRandomName(4))
}

func cacheFilePath(cacheDir string, shouldGzip bool) string {
	filename := fmt.Sprintf("%s/%s", cacheDir, generateRandomName(8)+".sql")
	return filenaming.EnsureFileSuffix(filename, shouldGzip)
}

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

func (dumper *Dumper) dumpToCacheFile() (string, func(), error) {
	file, cacheDir, err := createCacheFile(dumper.Job.Gzip)

	cleanup := func() {
		err := os.RemoveAll(cacheDir)
		if err != nil {
			log.Println("failed to remove cache dir after dump", err)
		}
	}

	if err != nil {
		return "", cleanup, err
	}

	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close cache file: %v", err)
		}
	}()

	err = dumper.dumpToFile(file)
	if err != nil {
		return "", cleanup, fmt.Errorf("failed to dump content to file: %w,", err)
	}

	// We have to close the file in defer function and returns filename instead of returing the fd (os.File)
	// Otherwise if we pass the fd and the storage func reuse the same fd, the file will be corrupted.
	return file.Name(), cleanup, nil
}

func (dumper *Dumper) dumpToFile(file io.Writer) error {
	job := dumper.Job
	driver, err := job.GetDBDriver()
	if err != nil {
		return fmt.Errorf("failed to get db driver %v", err)
	}

	if job.ViaSsh() {
		dumper := NewSshRunner(job.SshHost, job.SshKey, job.SshUser, job.Gzip, driver)
		return dumper.DumpToFile(file)
	} else {
		dumper := NewExecRunner(job.Gzip, driver)
		return dumper.DumpToFile(file)
	}
}

func (dumper *Dumper) save(cacheFile io.Reader) error {
	job := dumper.Job
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

func (dumper *Dumper) Dump() *config.JobResult {
	start := time.Now()
	result := &config.JobResult{}

	defer func() {
		elapsed := time.Since(start)
		result.Elapsed = elapsed
	}()

	result.JobName = dumper.Job.Name

	cacheFileName, cleanup, err := dumper.dumpToCacheFile()
	defer cleanup()

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

	err = dumper.save(cacheFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to store dump file %v", err)
	}

	return result
}

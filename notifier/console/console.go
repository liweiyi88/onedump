package console

import (
	"fmt"

	"github.com/liweiyi88/onedump/jobresult"
)

type Console struct{}

func New() *Console {
	return &Console{}
}

func (console *Console) Notify(results []*jobresult.JobResult) error {
	for _, result := range results {
		fmt.Println(result.String())
	}
	return nil
}

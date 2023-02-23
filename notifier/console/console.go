package console

import (
	"fmt"
	"strings"
)

type Console struct{}

func New() *Console {
	return &Console{}
}

func (console *Console) Notify(message []string) error {
	fmt.Println(strings.Join(message, "\r\n"))
	return nil
}

package env

import (
	"errors"
	"fmt"
	"os"
)

func ValidateEnvVars(vars []string) error {
	var errs error

	for _, v := range vars {
		if os.Getenv(v) == "" {
			errs = errors.Join(errs, fmt.Errorf("missing required environment variable %s", v))
		}
	}

	return errs
}

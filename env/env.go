package env

import (
	"errors"
	"fmt"
	"os"
)

const (
	AWS_REGION            = "AWS_REGION"
	AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
	AWS_SESSION_TOKEN     = "AWS_SESSION_TOKEN"
	DATABASE_DSN          = "DATABASE_DSN"
)

var ErrMissingEnv = errors.New("at least one env is required to resolve")

type AWSCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
}

func EnsureRequiredVars(vars []string) error {
	var errs error

	for _, v := range vars {
		if os.Getenv(v) == "" {
			errs = errors.Join(errs, fmt.Errorf("missing required environment variable %s", v))
		}
	}

	return errs
}

type EnvResolver struct {
	aws         bool
	databaseDSN bool
}

type resolverOption func(resolver *EnvResolver)

func NewEnvResolver(opts ...resolverOption) *EnvResolver {
	resolver := &EnvResolver{}

	for _, opt := range opts {
		opt(resolver)
	}

	return resolver
}

func WithAWS() resolverOption {
	return func(resolver *EnvResolver) {
		resolver.aws = true
	}
}

func WithDatabaseDSN() resolverOption {
	return func(resolver *EnvResolver) {
		resolver.databaseDSN = true
	}
}

type Values struct {
	AWSCredentials AWSCredentials
	DatabaseDSN    string
}

func (resolver *EnvResolver) Resolve() (Values, error) {
	requiredVars := make([]string, 0)

	if resolver.aws {
		requiredVars = append(requiredVars, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	}

	if resolver.databaseDSN {
		requiredVars = append(requiredVars, DATABASE_DSN)
	}

	if len(requiredVars) == 0 {
		return Values{}, ErrMissingEnv
	}

	if err := EnsureRequiredVars(requiredVars); err != nil {
		return Values{}, err
	}

	return Values{
		AWSCredentials: AWSCredentials{
			AccessKeyID:     os.Getenv(AWS_ACCESS_KEY_ID),
			SecretAccessKey: os.Getenv(AWS_SECRET_ACCESS_KEY),
			SessionToken:    os.Getenv(AWS_SESSION_TOKEN),
			Region:          os.Getenv(AWS_REGION),
		},
		DatabaseDSN: os.Getenv(DATABASE_DSN),
	}, nil
}

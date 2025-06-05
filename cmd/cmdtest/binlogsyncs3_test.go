package cmdtest

import (
	"os"
	"testing"

	"github.com/liweiyi88/onedump/env"
	"github.com/stretchr/testify/assert"
)

func TestValidateEnvVars(t *testing.T) {
	tests := []struct {
		name        string
		vars        []string
		setupEnv    func()
		cleanupEnv  func()
		expectError bool
	}{
		{
			name: "all variables present",
			vars: []string{"VAR1", "VAR2"},
			setupEnv: func() {
				os.Setenv("VAR1", "value1")
				os.Setenv("VAR2", "value2")
			},
			cleanupEnv: func() {
				os.Unsetenv("VAR1")
				os.Unsetenv("VAR2")
			},
			expectError: false,
		},
		{
			name: "one variable missing",
			vars: []string{"VAR1", "VAR2"},
			setupEnv: func() {
				os.Setenv("VAR1", "value1")
			},
			cleanupEnv: func() {
				os.Unsetenv("VAR1")
			},
			expectError: true,
		},
		{
			name:        "all variables missing",
			vars:        []string{"VAR1", "VAR2"},
			setupEnv:    func() {},
			cleanupEnv:  func() {},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupEnv()
			defer tt.cleanupEnv()

			err := env.ValidateEnvVars(tt.vars)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

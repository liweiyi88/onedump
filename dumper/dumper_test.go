package dumper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	options := newOptions("--skip-add-drop-table", "--skip-add-drop-table")

	assert.True(t, options.isEnabled("--skip-add-drop-table"))
	assert.True(t, options.isEnabled("--skip-add-drop-table"))
	assert.False(t, options.isEnabled("--some-other-option"))
}

package sliceutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunk(t *testing.T) {
	assert := assert.New(t)
	items := []int{1, 2, 3, 4, 5}

	t.Run("it should not chunk the slice if length is less than or equals to chunk size ", func(t *testing.T) {
		chunks := Chunk(items, 5)
		assert.Len(chunks, 1)
		assert.Equal(items, chunks[0])
	})

	t.Run("it should chunk the slice if length is greater than the chunk size", func(t *testing.T) {
		chunks := Chunk(items, 2)
		assert.Len(chunks, 3)
		assert.Equal([]int{1, 2}, chunks[0])
		assert.Equal([]int{3, 4}, chunks[1])
		assert.Equal([]int{5}, chunks[2])
	})
}

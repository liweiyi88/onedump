package sliceutil

func Chunk[T any](items []T, chunkSize int) [][]T {
	chunks := make([][]T, 0)

	if chunkSize <= 0 {
		return chunks
	}

	for start := 0; start < len(items); start += chunkSize {
		end := start + chunkSize
		if end > len(items) {
			end = len(items)
		}

		chunks = append(chunks, items[start:end])
	}

	return chunks
}

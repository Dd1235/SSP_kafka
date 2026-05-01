package payload

import (
	"bytes"
	"compress/gzip"
	"fmt"
)

func Visualize() {

	modes := []Mode{
		ModeRandom,
		ModeZeros,
		ModeText,
		ModeJSON,
		ModeLogline,
		ModeMixed,
	}
	size := 128
	seed := int64(42)

	fmt.Println("=== Payload Visualizaiton ===")

	for _, m := range modes {
		g, err := New(string(m), size, seed)
		if err != nil {
			panic(err)
		}
		buf := make([]byte, size)
		g.Next(buf, 1)
		fmt.Printf("Mode: %s\n", m)

		// show first 80 characters
		preview := printable(buf[:min(80, len(buf))])
		fmt.Printf("Preview: %s\n", preview)

		ratio := compressRatio(buf)
		fmt.Printf("Gzip ratio: %.2f\n", ratio)
		fmt.Println("------------------------------------------------------------------------------------")

	}
}

func VisualizeBatch() {

	modes := []Mode{
		ModeRandom,
		ModeZeros,
		ModeText,
		ModeJSON,
		ModeLogline,
		ModeMixed,
	}

	size := 128
	seed := int64(42)
	batchSize := 50

	for _, m := range modes {
		g, _ := New(string(m), size, seed)

		var batch bytes.Buffer

		for i := 0; i < batchSize; i++ {
			buf := make([]byte, size)
			g.Next(buf, uint64(i))
			batch.Write(buf)
		}

		ratio := compressRatio(batch.Bytes())

		fmt.Printf("Mode: %s | Batch ratio: %.2f\n", m, ratio)
		fmt.Println("Batch size bytes:", batch.Len())
		// 6400  = 128 * 50
	}

}

func compressRatio(data []byte) float64 {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(data)
	w.Close()
	return float64(len(data)) / float64(b.Len())
}

func printable(b []byte) string {
	out := make([]rune, len(b))
	for i, c := range b {
		// ascii printable character set, random may character characters fall out of this range
		if c < 32 {
			out[i] = '·' // dot for control
		} else if c > 126 {
			out[i] = '~' // high byte
		} else {
			out[i] = rune(c)
		}
	}
	return string(out)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

/*
Producer → Kafka broker → write() → OS page cache (RAM) → disk (eventually)

*/

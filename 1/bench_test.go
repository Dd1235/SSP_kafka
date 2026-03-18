package main

import "testing"

func BenchmarkFoo(b *testing.B) {

	n := 100
	// initializing of the code
	for i := 0; i <
		b.N; i++ {
		// the benchmark code
		ints := []int{} // non allocated slice of ints
		for i := 0; i < n; i++ {
			ints = append(ints, i)
		}
	}
}

package main

import (
	"fmt"
	"strings"
)

func main() {
	// Estimate the final size needed (e.g., 100 bytes)
	estimatedSize := 1024

	var sb strings.Builder
	fmt.Printf("Initial Capacity: %d, Length: %d\n", sb.Cap(), sb.Len())

	// Pre-allocate the buffer using Grow()
	sb.Grow(estimatedSize)

	fmt.Printf("After Grow capacity: %d, Length: %d\n", sb.Cap(), sb.Len())

	// Now write data to the builder
	sb.WriteString("This is the first part. ")
	sb.WriteString("And this is the second part.")
	// ... potentially many more writes

	fmt.Printf("Final Capacity: %d, Length: %d\n", sb.Cap(), sb.Len())
	fmt.Println("Result:", sb.String())
}

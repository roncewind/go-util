package main

import (
	"context"
	"fmt"
	"time"

	"github.com/roncewind/go-util/util"
)

// ----------------------------------------------------------------------------

func main() {
	orDoneExample()
}

// ----------------------------------------------------------------------------

// Example of how to use OrDone()... a fairly silly example.
// It creates a stream of ints, then prints the max that gets read in 100 microseconds
func orDoneExample() {
	ctx, cancel := context.WithCancel(context.Background())
	intStream := make(chan int)
	defer close(intStream)

	// goroutine that loads ints into a channel
	go func() {
		for i := 0; i < 500; i++ {
			intStream <- i
		}
	}()

	// goroutine waits 100 microseconds, then cancels the context
	go func() {
		time.Sleep(100 * time.Microsecond)
		cancel()
	}()

	max := 0
	// range over OrDone instead of implementing the for-select idiom
	for val := range util.OrDone(ctx, intStream) {
		max = val
	}
	fmt.Println(max)
}

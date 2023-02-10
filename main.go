package main

import (
	"context"
	"fmt"

	"github.com/roncewind/go-util/util"
)

// ----------------------------------------------------------------------------

func main() {
	orDoneExample()
}

// ----------------------------------------------------------------------------

// Example of how to use OrDone()
func orDoneExample() {
	ctx, cancel := context.WithCancel(context.Background())
	intStream := make(chan interface{})
	go func() {
		for i := 0; i < 20; i++ {
			intStream <- i
		}
	}()

	// range over OrDone instead of implementing the for-select idiom
	for val := range util.OrDone(ctx, intStream) {
		fmt.Println(val)
		if val == 10 {
			cancel()
		}
	}

}

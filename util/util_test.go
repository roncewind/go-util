package util

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// ----------------------------------------------------------------------------

// test normal operation of OrDone
func TestUtil_OrDone(t *testing.T) {
	intCount := 1000
	cancelled := false
	ctx, cancel := context.WithCancel(context.Background())
	intStream := make(chan int)

	// goroutine that loads ints into a channel
	go func() {
		for i := 0; i < intCount; i++ {
			intStream <- i
		}
	}()

	// goroutine waits 100 microseconds, then cancels the context
	go func() {
		time.Sleep(100 * time.Microsecond)
		cancelled = true
		cancel()
	}()

	max := 0
	// range over OrDone instead of implementing the for-select idiom
	for val := range OrDone(ctx, intStream) {
		max = val
	}
	fmt.Println(max, intCount)
	if !cancelled {
		t.Fatal("expected context to cancel before end of test")
	}
}

// ----------------------------------------------------------------------------

// test OrDone on closed stream
func TestUtil_OrDone_closedStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	intStream := make(chan int)
	close(intStream)

	// goroutine waits 100 microseconds, then cancels the context
	go func() {
		time.Sleep(1000 * time.Microsecond)
		cancel()
	}()

	max := 0
	// range over OrDone instead of implementing the for-select idiom
	for val := range OrDone(ctx, intStream) {
		max = val
		if max > 0 {
			t.Fatal("OrDone pulling from closed stream")
		}
	}

}

// ----------------------------------------------------------------------------

// test Tee
func TestUtil_Tee(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	intStream := make(chan int)
	go func() {
		intStream <- 42
		close(intStream)
	}()

	out1, out2 := Tee(ctx, intStream)
	val1 := <-out1
	val2 := <-out2

	if val1 != val2 {
		t.Fatal("Tee'd streams not the same")
	}
}

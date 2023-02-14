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

// ----------------------------------------------------------------------------

// utility function used for testing Or
func sig(after time.Duration) <-chan interface{} {
	c := make(chan interface{})
	go func() {
		defer close(c)
		time.Sleep(after)
	}()
	return c
}

// ----------------------------------------------------------------------------

// test Or
func TestUtil_Or_1(t *testing.T) {
	start := time.Now()
	<-Or(
		sig(1 * time.Second),
	)
	if since := int(time.Since(start) / time.Second); since > 1 {
		t.Fatalf("failed to select the correct channel: %d", since)
	}
}

// ----------------------------------------------------------------------------

// test Or
func TestUtil_Or_2(t *testing.T) {
	start := time.Now()
	<-Or(
		sig(1*time.Second),
		sig(1*time.Minute),
	)
	if since := int(time.Since(start) / time.Second); since > 1 {
		t.Fatalf("failed to select the correct channel: %d", since)
	}
}

// ----------------------------------------------------------------------------

// test Or
func TestUtil_Or_5(t *testing.T) {
	start := time.Now()
	<-Or(
		sig(2*time.Hour),
		sig(5*time.Minute),
		sig(1*time.Second),
		sig(1*time.Hour),
		sig(1*time.Minute),
	)
	if since := int(time.Since(start) / time.Second); since > 1 {
		t.Fatalf("failed to select the correct channel: %d", since)
	}
}

// ----------------------------------------------------------------------------

// test FanIn
func TestUtil_FanIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	intStream1 := make(chan int)
	intStream2 := make(chan int)
	defer close(intStream1)
	defer close(intStream2)

	go func() {
		intStream1 <- 1
		intStream2 <- 41
	}()

	joinedStream := FanIn(ctx, intStream1, intStream2)
	val1 := <-joinedStream
	val2 := <-joinedStream

	if val1+val2 != 42 {
		t.Fatal("did not FanIn properly")
	}

}

// ----------------------------------------------------------------------------

// test FanIn context cancelled
func TestUtil_FanIn_cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	intStream1 := make(chan int)
	intStream2 := make(chan int)
	defer close(intStream1)
	defer close(intStream2)

	joinedStream := FanIn(ctx, intStream1, intStream2)
	_, ok1 := <-joinedStream
	_, ok2 := <-joinedStream

	if ok1 && ok2 {
		t.Fatal("did not FanIn properly with cancelled context")
	}

}

// ----------------------------------------------------------------------------

// test Bridge
func TestUtil_Bridge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	generateValues := func() <-chan <-chan int {
		chanStream := make(chan (<-chan int))
		go func() {
			defer close(chanStream)
			for i := 0; i < 2; i++ {
				stream := make(chan int, 1)
				stream <- i
				close(stream)
				chanStream <- stream
			}
		}()
		return chanStream
	}

	accumulator := 0
	for v := range Bridge(ctx, generateValues()) {
		accumulator += v
	}

	if accumulator != 1 {
		t.Fatal("error in Bridge")
	}
}

// ----------------------------------------------------------------------------

// test Bridge
func TestUtil_Bridge_cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	generateValues := func() <-chan <-chan int {
		chanStream := make(chan (<-chan int))
		go func() {
			defer close(chanStream)
			for i := 0; i < 2; i++ {
				stream := make(chan int, 1)
				stream <- i
				close(stream)
				chanStream <- stream
			}
		}()
		return chanStream
	}

	v := Bridge(ctx, generateValues())
	if len(v) != 0 {
		t.Fatal("error in Bridge with cancelled context")
	}
}

// ----------------------------------------------------------------------------

// test Repeat generator
func TestUtil_Repeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := Repeat(ctx, "foo")
	val1 := <-c
	val2 := <-c
	if val1 != val2 {
		t.Fatal("values were not repeated as expected")
	}
}

// ----------------------------------------------------------------------------

// test Repeat generator with cancelled context
func TestUtil_Repeat_cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := Repeat(ctx, "foo")
	_, ok1 := <-c
	_, ok2 := <-c
	if ok1 && ok2 {
		t.Fatal("cancel Repeat failed")
	}
}

// ----------------------------------------------------------------------------

// test Repeat generator
func TestUtil_RepeatFn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn := func() int {
		return 1
	}
	c := RepeatFn(ctx, fn)
	val1 := <-c
	val2 := <-c
	if val1 != val2 {
		t.Fatal("function results were not repeated as expected")
	}
}

// ----------------------------------------------------------------------------

// test Repeat generator with cancelled context
func TestUtil_RepeatFn_cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fn := func() int {
		return 1
	}
	c := RepeatFn(ctx, fn)
	_, ok1 := <-c
	_, ok2 := <-c
	if ok1 && ok2 {
		t.Fatal("cancel RepeatFn failed")
	}
}

// ----------------------------------------------------------------------------

// test Take generator
func TestUtil_Take(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := Take(ctx, Repeat(ctx, "foo"), 2)
	val1 := <-c
	val2 := <-c
	if val1 != val2 {
		t.Fatal("values were not repeated as expected")
	}

	_, ok := <-c
	if ok {
		t.Fatal("channel should be closed")
	}
}

// ----------------------------------------------------------------------------

// test Take generator
func TestUtil_Take_cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := Take(ctx, Repeat(ctx, "foo"), 2)
	_, ok1 := <-c
	_, ok2 := <-c
	if ok1 && ok2 {
		t.Fatal("cancel Take failed")
	}
}

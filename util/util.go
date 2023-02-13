package util

import (
	"context"
	"sync"
)

// ----------------------------------------------------------------------------
// Channels based on Katherine Cox-Buday's Concurrency in Go
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------

// OrDone encapsulates the for-select idiom used for many goroutines
// the idea is that it makes the code easier to read
func OrDone[T any](ctx context.Context, c <-chan T) <-chan T {
	valueStream := make(chan T)
	go func() {
		defer close(valueStream)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-c:
				if !ok {
					return
				}
				select {
				case valueStream <- v:
				case <-ctx.Done():
				}
			}
		}
	}()
	return valueStream
}

// ----------------------------------------------------------------------------

// Tee takes one channel of objects and splits it into two channels,
// duplicating the channels.  Much like the `tee` tool in *nix
func Tee[T any](ctx context.Context, in <-chan T) (_, _ <-chan T) {
	out1 := make(chan T)
	out2 := make(chan T)
	go func() {
		defer close(out1)
		defer close(out2)
		for val := range OrDone(ctx, in) {
			// create shadow vars
			var out1, out2 = out1, out2
			for i := 0; i < 2; i++ {
				select {
				case <-ctx.Done():
				case out1 <- val:
					out1 = nil
				case out2 <- val:
					out2 = nil
				}
			}
		}
	}()
	return out1, out2
}

// ----------------------------------------------------------------------------

// Or returns a channel that combines all the specified channels.
func Or[T any](channels ...<-chan T) <-chan T {
	if len(channels) == 1 {
		return channels[0]
	}
	// When using generics Or cannot be called with zero arguments since the
	// type cannot be inferred
	// switch len(channels) {
	// case 0:
	// 	return nil
	// case 1:
	// 	return channels[0]
	// }

	orDone := make(chan T)
	go func() {
		defer close(orDone)

		switch len(channels) {
		case 2:
			select {
			case <-channels[0]:
			case <-channels[1]:
			}
		default:
			select {
			case <-channels[0]:
			case <-channels[1]:
			case <-channels[2]:
			case <-Or(append(channels[3:], orDone)...):
			}
		}
	}()
	return orDone
}

// ----------------------------------------------------------------------------

// FanIn joins several channels into one
func FanIn[T any](ctx context.Context, streams ...<-chan T) <-chan T {
	joinedStream := make(chan T)

	var wg sync.WaitGroup
	wg.Add(len(streams))

	for _, s := range streams {
		s := s

		go func() {
			defer wg.Done()
			for {
				select {
				case stream := <-s:
					joinedStream <- stream
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(joinedStream)
	}()
	return joinedStream
}

// ----------------------------------------------------------------------------
// Generators based on Katherine Cox-Buday's Concurrency in Go
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------

// generator that repeats over and over the values it is given
func Repeat[T any](ctx context.Context, values ...T) <-chan T {
	valueStream := make(chan T)
	go func() {
		defer close(valueStream)
		for {
			for _, v := range values {
				select {
				case <-ctx.Done():
					return
				case valueStream <- v:
				}
			}
		}
	}()
	return valueStream
}

// ----------------------------------------------------------------------------

// generator that takes the specified number of elements from a stream
func Take[T any](ctx context.Context, valueStream <-chan T, num int) <-chan T {
	takeStream := make(chan T)
	go func() {
		defer close(takeStream)
		for i := 0; i < num; i++ {
			select {
			case <-ctx.Done():
				return
			case takeStream <- <-valueStream:
			}
		}
	}()
	return takeStream
}

package util

import "context"

// ----------------------------------------------------------------------------
// Generators based on Katherine Cox-Buday's "Concurrency in Go"
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

// generator that repeats over and over the values it is given
func RepeatFn[T any](ctx context.Context, fn func() T) <-chan T {
	valueStream := make(chan T)
	go func() {
		defer close(valueStream)
		for {
			select {
			case <-ctx.Done():
				return
			case valueStream <- fn():
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

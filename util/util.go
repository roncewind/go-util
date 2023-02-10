package util

import "context"

// ----------------------------------------------------------------------------

// OrDone encapsulates the for-select idiom used for many goroutines
// the idea is that it makes the code easier to read
func OrDone[T interface{}](ctx context.Context, c <-chan T) <-chan T {
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

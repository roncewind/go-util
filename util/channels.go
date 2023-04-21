package util

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// ----------------------------------------------------------------------------
// Channels some of which is based on Katherine Cox-Buday's "Concurrency in Go"
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

// Bridge joins transforms a series of channels into a single channel
func Bridge[T any](ctx context.Context, chanStream <-chan <-chan T) <-chan T {
	valStream := make(chan T)
	go func() {
		defer close(valStream)
		for {
			var stream <-chan T
			select {
			case maybeStream, ok := <-chanStream:
				if !ok {
					return
				}
				stream = maybeStream
			case <-ctx.Done():
				return
			}
			for val := range OrDone(ctx, stream) {
				select {
				case valStream <- val:
				case <-ctx.Done():
				}
			}
		}
	}()
	return valStream
}

// ----------------------------------------------------------------------------

// Shuffle items coming through a channel.
// input a channel of items to be shuffled.
// output a channel of shuffled items.
// target distance is how, on average, far each item should move.
// there's no guarantee that a particular item will move that far, but it
// is used as general guidance.
// Beware: This function incurs a 1μs per item performance hit.
func Shuffle[T any](ctx context.Context, in chan T, targetDistance int) <-chan T {

	bufferSize = int(math.Round(float64(targetDistance) * 1.7))
	if bufferSize <= 0 {
		bufferSize = 1
	}
	// fmt.Println("bufferSize:", bufferSize)
	count := 0
	recordChan := make(chan *record, 5)
	outChan := make(chan T)
	go func() {
		for item := range OrDone(ctx, in) {
			count++
			r := record{
				item:         item,
				count:        0,
				initPosition: count,
			}
			recordChan <- &r
		}
		close(recordChan)
		// fmt.Println("Total records received:", count)
	}()
	go doShuffle(ctx, recordChan, outChan)
	return outChan
}

// ----------------------------------------------------------------------------

// internal type for tracking the record
type record struct {
	item         any
	count        int
	initPosition int
}

var bufferSize int = 1000
var delayInMicros time.Duration = 1

// ----------------------------------------------------------------------------

// shuffle the records
func doShuffle[T any](ctx context.Context, in chan *record, out chan T) {

	var wg sync.WaitGroup
	readCount := 0
	writeCount := 0
	distance := 0
	var sliceMutex sync.RWMutex
	recordBuffer := make([]*record, bufferSize)
	getRecordPtr := func(slot int) *record {
		sliceMutex.RLock()
		rptr := recordBuffer[slot]
		sliceMutex.RUnlock()
		return rptr
	}
	doneReading := make(chan struct{})
	wg.Add(2)
	// read from the in channel randomly putting records in slice slots
	go func() {
		r := rand.New(rand.NewSource(time.Now().Unix()))
		for item := range OrDone(ctx, in) {
			readCount++
			slot := r.Intn(bufferSize)
			for getRecordPtr(slot) != nil {
				slot = r.Intn(bufferSize)
			}
			sliceMutex.Lock()
			recordBuffer[slot] = item
			sliceMutex.Unlock()
		}
		doneReading <- struct{}{}
		wg.Done()
	}()

	// randomly read records from slice slots and put them in the out channel
	go func() {
		r := rand.New(rand.NewSource(time.Now().Unix()))
		var item *record
	stillReading:
		for {
			select {
			case <-doneReading:
				break stillReading
			default:
				slot := r.Intn(bufferSize)
				for getRecordPtr(slot) == nil {
					slot = r.Intn(bufferSize)
				}

				sliceMutex.Lock()
				item, recordBuffer[slot] = recordBuffer[slot], nil
				sliceMutex.Unlock()
				writeCount++
				distance += int(math.Abs(float64(writeCount - item.initPosition)))
				// pause before each round to allow the writer to fill in the slice
				time.Sleep(delayInMicros * time.Microsecond)
				// fmt.Println(item.initPosition, "-->", writeCount, "d=", writeCount-item.initPosition)
				out <- item.item.(T)

			}
		}
		// flush the rest from the buffer
		sliceMutex.Lock()
		for i, item := range recordBuffer {
			if item != nil {
				writeCount++
				distance += int(math.Abs(float64(writeCount - item.initPosition)))
				// fmt.Println(item.initPosition, "-->", writeCount, "d=", writeCount-item.initPosition)
				out <- item.item.(T)
				recordBuffer[i] = nil
			}
		}
		sliceMutex.Unlock()
		close(out)
		wg.Done()
	}()
	wg.Wait()
	// fmt.Println("Total records written:", writeCount)
	// fmt.Println("Total distance:", distance)
	// fmt.Println("Average distance:", distance/writeCount)
}

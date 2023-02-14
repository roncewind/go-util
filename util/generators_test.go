package util

import (
	"context"
	"testing"
)

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

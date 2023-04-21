package util

import (
	"errors"
	"fmt"
	"testing"
)

// ----------------------------------------------------------------------------

// test normal operation of WrapError
func TestUtil_WrapError(t *testing.T) {
	var err interface{} = WrapError(errors.New("inner"), "outer %s", "foo")
	if _, ok := err.(SzError); ok {
		fmt.Println(err)
	} else {
		t.Fatal("WrapError did not create an SzError")
	}

}

// test normal operation of Error
func TestUtil_Error(t *testing.T) {
	err := SzError{
		Inner:   errors.New("inner"),
		Message: "message",
	}

	if err.Error() != "message" {
		t.Fatal("Error() did not return the correct message")
	}
}

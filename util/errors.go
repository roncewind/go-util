package util

import (
	"fmt"
	"runtime/debug"
)

type SzError struct {
	Inner      error
	Message    string
	StackTrace string
	Misc       map[string]any
}

func WrapError(err error, messagef string, msgArgs ...any) SzError {
	return SzError{
		Inner:      err,
		Message:    fmt.Sprintf(messagef, msgArgs...),
		StackTrace: string(debug.Stack()),
		Misc:       make(map[string]any),
	}
}

func (err SzError) Error() string {
	return err.Message
}

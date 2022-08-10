package resp

import (
	"errors"
	"fmt"
)

type ParseError struct {
	expected []RespT
	got      RespT
	e        error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("got %v but expected %v", e.got, e.expected)
}

func (e *ParseError) Unwrap() error {
	return e.e
}

var ErrParsing = errors.New("ParseError")

func NewParseError(got RespT, expected ...RespT) *ParseError {
	return &ParseError{
		expected: expected,
		got:      got,
		e:        ErrParsing,
	}
}

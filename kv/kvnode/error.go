package kvnode

import "fmt"

// ErrorCode represents the error code
type ErrorCode int

const (
	// ErrCodePeerNotFound is returned when target peer is not found
	ErrCodePeerNotFound ErrorCode = iota
)

// Error represents an error with code
type Error struct {
	Code ErrorCode
	Msg  string
}

// NewError creates a new Error
func NewError(code ErrorCode, msg string) *Error {
	return &Error{
		Code: code,
		Msg:  msg,
	}
}

// Error implements the error interface
func (e *Error) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Msg)
}

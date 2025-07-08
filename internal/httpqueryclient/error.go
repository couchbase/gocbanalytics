package httpqueryclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrAnalytics occurs from a client-server interaction with the Analytics service.
	ErrAnalytics = errors.New("analytics error")

	// ErrContextDeadlineWouldBeExceeded is returned when a Deadline set on an operation
	// would be exceeded if the operation were sent to the server. It wraps
	// context.DeadlineExceeded.
	ErrContextDeadlineWouldBeExceeded = fmt.Errorf(
		"operation not sent to server, as timeout would be exceeded: %w",
		context.DeadlineExceeded,
	)

	// ErrTimeout occurs when an operation does not receive a response in a timely manner.
	ErrTimeout = errors.New("operation has timed out")

	// ErrInvalidCredential occurs when an invalid set of credentials is provided for a service.
	ErrInvalidCredential = errors.New("an invalid set of credentials was provided")

	// ErrServiceUnavailable occurs when the Analytics service, or a part of the system in the path to it, is unavailable.
	ErrServiceUnavailable = errors.New("service unavailable")
)

// ErrorDesc represents specific Analytics error data.
type ErrorDesc struct {
	Code    uint32
	Message string
	Retry   bool
}

// MarshalJSON implements the Marshaler interface.
func (e ErrorDesc) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(struct {
		Code    uint32 `json:"code"`
		Message string `json:"msg"`
	}{
		Code:    e.Code,
		Message: e.Message,
	})
	if err != nil {
		return nil, newObfuscateErrorWrapper("failed to marshal error desc", err)
	}

	return b, nil
}

// QueryError represents an error returned from an Analytics query.
type QueryError struct {
	InnerError       error
	Statement        string
	Errors           []ErrorDesc
	LastErrorCode    uint32
	LastErrorMsg     string
	Endpoint         string
	ErrorText        string
	HTTPResponseCode int
}

func newAnalyticsError(innerError error, statement string, endpoint string, responseCode int) *QueryError {
	return &QueryError{
		InnerError:       innerError,
		Statement:        statement,
		Errors:           nil,
		LastErrorCode:    0,
		LastErrorMsg:     "",
		Endpoint:         endpoint,
		ErrorText:        "",
		HTTPResponseCode: responseCode,
	}
}

func (e QueryError) withErrorText(errText string) *QueryError {
	e.ErrorText = errText

	return &e
}

func (e QueryError) withLastDetail(code uint32, msg string) *QueryError {
	e.LastErrorCode = code
	e.LastErrorMsg = msg

	return &e
}

func (e QueryError) withErrors(errors []ErrorDesc) *QueryError {
	e.Errors = errors

	return &e
}

// MarshalJSON implements the Marshaler interface.
func (e QueryError) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(struct {
		InnerError       string      `json:"msg,omitempty"`
		Statement        string      `json:"statement,omitempty"`
		Errors           []ErrorDesc `json:"errors,omitempty"`
		LastCode         uint32      `json:"lastCode,omitempty"`
		LastMessage      string      `json:"lastMsg,omitempty"`
		Endpoint         string      `json:"endpoint,omitempty"`
		HTTPResponseCode int         `json:"status_code,omitempty"`
	}{
		InnerError:       e.InnerError.Error(),
		Statement:        e.Statement,
		Errors:           e.Errors,
		LastCode:         e.LastErrorCode,
		LastMessage:      e.LastErrorMsg,
		Endpoint:         e.Endpoint,
		HTTPResponseCode: e.HTTPResponseCode,
	})
	if err != nil {
		return nil, newObfuscateErrorWrapper("failed to marshal error", err)
	}

	return b, nil
}

// QueryError returns the string representation of this error.
func (e QueryError) Error() string {
	errBytes, _ := json.Marshal(struct {
		InnerError       error       `json:"-"`
		Statement        string      `json:"statement,omitempty"`
		Errors           []ErrorDesc `json:"errors,omitempty"`
		LastCode         uint32      `json:"lastCode,omitempty"`
		LastMessage      string      `json:"lastMsg,omitempty"`
		Endpoint         string      `json:"endpoint,omitempty"`
		ErrorText        string      `json:"error_text,omitempty"`
		HTTPResponseCode int         `json:"status_code,omitempty"`
	}{
		InnerError:       e.InnerError,
		Statement:        e.Statement,
		Errors:           e.Errors,
		LastCode:         e.LastErrorCode,
		LastMessage:      e.LastErrorMsg,
		Endpoint:         e.Endpoint,
		ErrorText:        e.ErrorText,
		HTTPResponseCode: e.HTTPResponseCode,
	})
	// TODO: Log here

	return e.InnerError.Error() + " | " + string(errBytes)
}

// Unwrap returns the underlying reason for the error
func (e QueryError) Unwrap() error {
	return e.InnerError
}

type obfuscateErrorWrapper struct {
	InnerError error
	Message    string
}

func newObfuscateErrorWrapper(message string, innerError error) *obfuscateErrorWrapper {
	return &obfuscateErrorWrapper{
		InnerError: innerError,
		Message:    message,
	}
}

func (e *obfuscateErrorWrapper) Error() string {
	return fmt.Sprintf("%s: %s", e.Message, e.InnerError)
}

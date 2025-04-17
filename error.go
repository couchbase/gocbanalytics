package cbcolumnar

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrColumnar is the base error for any Columnar error that is not captured by a more specific error.
var ErrColumnar = errors.New("columnar error")

// ErrInvalidCredential occurs when invalid credentials are provided leading to errors in things like authentication.
var ErrInvalidCredential = errors.New("invalid credential")

// ErrTimeout occurs when a timeout is reached while waiting for a response.
// This is returned when a server timeout occurs, or an operation fails to be sent within the dispatch timeout.
var ErrTimeout = errors.New("timeout error")

// ErrQuery occurs when a server error is encountered while executing a query, excluding errors that caught by
// ErrInvalidCredential or ErrTimeout.
var ErrQuery = errors.New("query error")

// ErrInvalidArgument occurs when an invalid argument is provided to a function.
var ErrInvalidArgument = errors.New("invalid argument")

// ErrClosed occurs when an entity was used after it was closed.
var ErrClosed = errors.New("closed")

// ErrUnmarshal occurs when an entity could not be unmarshalled.
var ErrUnmarshal = errors.New("unmarshalling error")

type columnarErrorDesc struct {
	Code    uint32
	Message string
}

func (e columnarErrorDesc) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(struct {
		Code    uint32 `json:"code"`
		Message string `json:"msg"`
	}{
		Code:    e.Code,
		Message: e.Message,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal columnar error description: %s", err) // nolint: err113, errorlint
	}

	return b, nil
}

// ColumnarError occurs when an error is encountered while interacting with the Columnar service.
type ColumnarError struct {
	cause   error
	message string

	errors           []columnarErrorDesc
	statement        string
	endpoint         string
	httpResponseCode int
}

// nolint: unused
func newColumnarError(statement, endpoint string, statusCode int) ColumnarError {
	return ColumnarError{
		cause:            nil,
		errors:           nil,
		statement:        statement,
		endpoint:         endpoint,
		message:          "",
		httpResponseCode: statusCode,
	}
}

func (e ColumnarError) withMessage(message string) *ColumnarError {
	e.message = message

	return &e
}

func (e ColumnarError) withErrors(errors []columnarErrorDesc) *ColumnarError {
	e.errors = errors

	return &e
}

func (e ColumnarError) withCause(cause error) *ColumnarError {
	e.cause = cause

	return &e
}

// Error returns the string representation of a Columnar error.
func (e ColumnarError) Error() string {
	errBytes, serErr := json.Marshal(struct {
		Statement        string              `json:"statement,omitempty"`
		Errors           []columnarErrorDesc `json:"errors,omitempty"`
		Message          string              `json:"message,omitempty"`
		Endpoint         string              `json:"endpoint,omitempty"`
		HTTPResponseCode int                 `json:"status_code,omitempty"`
	}{
		Statement:        e.statement,
		Errors:           e.errors,
		Message:          e.message,
		Endpoint:         e.endpoint,
		HTTPResponseCode: e.httpResponseCode,
	})
	if serErr != nil {
		logErrorf("failed to serialize error to json: %s", serErr.Error())
	}

	cause := e.cause
	if cause == nil {
		cause = ErrColumnar
	}

	return cause.Error() + " | " + string(errBytes)
}

// Unwrap returns the underlying reason for the error.
func (e ColumnarError) Unwrap() error {
	if e.cause == nil {
		return ErrColumnar
	}

	return e.cause
}

// QueryError occurs when an error is returned in the errors field of the response body of a response
// from the query server.
type QueryError struct {
	cause   *ColumnarError
	code    int
	message string
}

// Code returns the error code from the server for this error.
func (e QueryError) Code() int {
	return e.code
}

// Message returns the error message from the server for this error.
func (e QueryError) Message() string {
	return e.message
}

// Error returns the string representation of a query error.
func (e QueryError) Error() string {
	return fmt.Errorf("%w", e.cause).Error()
}

// Unwrap returns the underlying reason for the error.
func (e QueryError) Unwrap() error {
	return e.cause
}

func (e QueryError) withErrors(errors []columnarErrorDesc) *QueryError {
	e.cause.errors = errors

	return &e
}

// nolint: unused
func newQueryError(statement, endpoint string, statusCode int, code int, message string) QueryError {
	return QueryError{
		cause: &ColumnarError{
			cause:            ErrQuery,
			errors:           nil,
			statement:        statement,
			endpoint:         endpoint,
			message:          "",
			httpResponseCode: statusCode,
		},
		code:    code,
		message: message,
	}
}

type invalidArgumentError struct {
	ArgumentName string
	Reason       string
}

func (e invalidArgumentError) Error() string {
	return fmt.Sprintf("%s %s - %s", e.Unwrap(), e.ArgumentName, e.Reason)
}

func (e invalidArgumentError) Unwrap() error {
	return ErrInvalidArgument
}

type unmarshalError struct {
	Reason string
}

func (e unmarshalError) Error() string {
	return fmt.Sprintf("failed to unmarshal - %s", e.Reason)
}

func (e unmarshalError) Unwrap() error {
	return ErrUnmarshal
}

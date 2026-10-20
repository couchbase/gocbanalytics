package fiterror

import (
	"context"
	"errors"

	cbanalytics "github.com/couchbase/gocbanalytics"
	protoErrors "github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol/errors"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol/result"
)

func ParseSDKError(err error) *protoErrors.Error {
	if errors.Is(err, cbanalytics.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return &protoErrors.Error{
			Error: &protoErrors.Error_Columnar{
				Columnar: &protoErrors.ColumnarError{
					Cause: nil,
					SubException: &protoErrors.SubColumnarError{
						SubError: &protoErrors.SubColumnarError_TimeoutException{
							TimeoutException: &protoErrors.TimeoutException{},
						},
					},
					AsString: err.Error(),
				},
			},
		}
	}

	var queryErr *cbanalytics.QueryError
	if errors.As(err, &queryErr) {
		var subException *protoErrors.SubColumnarError
		if errors.Is(err, cbanalytics.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			subException = &protoErrors.SubColumnarError{
				SubError: &protoErrors.SubColumnarError_TimeoutException{
					TimeoutException: &protoErrors.TimeoutException{},
				},
			}
		} else {
			subException = &protoErrors.SubColumnarError{
				SubError: &protoErrors.SubColumnarError_QueryException{
					QueryException: &protoErrors.QueryException{
						ErrorCode:     int32(queryErr.Code()),
						ServerMessage: queryErr.Message(),
					},
				},
			}
		}

		return &protoErrors.Error{
			Error: &protoErrors.Error_Columnar{
				Columnar: &protoErrors.ColumnarError{
					Cause:        nil, // TODO: is this right?
					SubException: subException,
					AsString:     err.Error(),
				},
			},
		}
	}

	var analyticsErr *cbanalytics.AnalyticsError
	if errors.As(err, &analyticsErr) {
		var subException *protoErrors.SubColumnarError

		switch {
		case errors.Is(err, cbanalytics.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
			subException = &protoErrors.SubColumnarError{
				SubError: &protoErrors.SubColumnarError_TimeoutException{
					TimeoutException: &protoErrors.TimeoutException{},
				},
			}
		case errors.Is(err, cbanalytics.ErrInvalidCredential):
			subException = &protoErrors.SubColumnarError{
				SubError: &protoErrors.SubColumnarError_InvalidCredentialException{
					InvalidCredentialException: &protoErrors.InvalidCredentialException{},
				},
			}
		case errors.Is(err, cbanalytics.ErrQueryNotFound):
			subException = &protoErrors.SubColumnarError{
				SubError: &protoErrors.SubColumnarError_QueryNotFoundException{
					QueryNotFoundException: &protoErrors.QueryNotFoundException{},
				},
			}
		}

		return &protoErrors.Error{
			Error: &protoErrors.Error_Columnar{
				Columnar: &protoErrors.ColumnarError{
					Cause:        nil, // TODO: is this right?
					SubException: subException,
					AsString:     err.Error(),
				},
			},
		}
	}

	return &protoErrors.Error{
		Error: &protoErrors.Error_Platform{
			Platform: &protoErrors.PlatformError{
				Type:     protoErrors.PlatformErrorType_PLATFORM_ERROR_OTHER, // TODO: expose other error types
				AsString: err.Error(),
			},
		},
	}
}

func ConvertSDKErrorToFailureResponse(err error) *result.EmptyResultOrFailureResponse_Error {
	return &result.EmptyResultOrFailureResponse_Error{
		Error: ParseSDKError(err),
	}
}

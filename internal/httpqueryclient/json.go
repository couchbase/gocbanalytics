package httpqueryclient

import "encoding/json"

type jsonAnalyticsError struct {
	Code  uint32 `json:"code"`
	Msg   string `json:"msg"`
	Retry bool   `json:"retriable"`
}

type jsonAnalyticsErrorResponse struct {
	Errors json.RawMessage `json:"errors"`
}

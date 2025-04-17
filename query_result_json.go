package cbcolumnar

import (
	"time"
)

type jsonAnalyticsMetrics struct {
	ElapsedTime      string `json:"elapsedTime"`
	ExecutionTime    string `json:"executionTime"`
	ResultCount      uint64 `json:"resultCount"`
	ResultSize       uint64 `json:"resultSize"`
	MutationCount    uint64 `json:"mutationCount,omitempty"`
	SortCount        uint64 `json:"sortCount,omitempty"`
	ErrorCount       uint64 `json:"errorCount,omitempty"`
	WarningCount     uint64 `json:"warningCount,omitempty"`
	ProcessedObjects uint64 `json:"processedObjects,omitempty"`
}

type jsonAnalyticsWarning struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}

type jsonAnalyticsResponse struct {
	RequestID       string                 `json:"requestID"`
	ClientContextID string                 `json:"clientContextID"`
	Status          string                 `json:"status"`
	Warnings        []jsonAnalyticsWarning `json:"warnings"`
	Metrics         jsonAnalyticsMetrics   `json:"metrics"`
	Signature       interface{}            `json:"signature"`
	Handle          string                 `json:"handle,omitempty"`
}

func (meta *QueryMetadata) fromData(data jsonAnalyticsResponse) {
	metrics := QueryMetrics{
		ElapsedTime:      0,
		ExecutionTime:    0,
		ResultCount:      0,
		ResultSize:       0,
		ProcessedObjects: 0,
	}
	metrics.fromData(data.Metrics)

	warnings := make([]QueryWarning, len(data.Warnings))
	for wIdx, jsonWarning := range data.Warnings {
		warnings[wIdx].fromData(jsonWarning)
	}

	meta.RequestID = data.RequestID
	meta.Metrics = metrics
	meta.Warnings = warnings
}

func (metrics *QueryMetrics) fromData(data jsonAnalyticsMetrics) {
	elapsedTime, err := time.ParseDuration(data.ElapsedTime)
	if err != nil {
		logDebugf("Failed to parse query metrics elapsed time: %s", err)
	}

	executionTime, err := time.ParseDuration(data.ExecutionTime)
	if err != nil {
		logDebugf("Failed to parse query metrics execution time: %s", err)
	}

	metrics.ElapsedTime = elapsedTime
	metrics.ExecutionTime = executionTime
	metrics.ResultCount = data.ResultCount
	metrics.ResultSize = data.ResultSize
	metrics.ProcessedObjects = data.ProcessedObjects
}

func (warning *QueryWarning) fromData(data jsonAnalyticsWarning) {
	warning.Code = data.Code
	warning.Message = data.Message
}

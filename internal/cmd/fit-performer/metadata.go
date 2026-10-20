package main

import (
	cbanalytics "github.com/couchbase/gocbanalytics"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol/query"
	"google.golang.org/protobuf/types/known/durationpb"
)

func ParseMetadata(meta *cbanalytics.QueryMetadata) *query.QueryResultMetadataResponse_QueryMetadata {
	resMeta := &query.QueryResultMetadataResponse_QueryMetadata{
		RequestId: meta.RequestID,
		Warnings:  make([]*query.QueryResultMetadataResponse_QueryMetadata_Warning, len(meta.Warnings)),
		Metrics: &query.QueryResultMetadataResponse_QueryMetadata_Metrics{
			ElapsedTime:      durationpb.New(meta.Metrics.ElapsedTime),
			ExecutionTime:    durationpb.New(meta.Metrics.ExecutionTime),
			ResultCount:      meta.Metrics.ResultCount,
			ResultSize:       meta.Metrics.ResultSize,
			ProcessedObjects: meta.Metrics.ProcessedObjects,
		},
	}

	for i, warning := range meta.Warnings {
		resMeta.Warnings[i] = &query.QueryResultMetadataResponse_QueryMetadata_Warning{
			Code:    warning.Code,
			Message: warning.Message,
		}
	}

	return resMeta
}

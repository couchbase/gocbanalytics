package cbcolumnar

import (
	"time"
)

// QueryMetrics encapsulates various metrics gathered during a queries execution.
type QueryMetrics struct {
	ElapsedTime      time.Duration
	ExecutionTime    time.Duration
	ResultCount      uint64
	ResultSize       uint64
	ProcessedObjects uint64
}

// QueryWarning encapsulates any warnings returned by a query.
type QueryWarning struct {
	Code    uint32
	Message string
}

// QueryMetadata provides access to the meta-data properties of a query result.
type QueryMetadata struct {
	RequestID string
	Metrics   QueryMetrics
	Warnings  []QueryWarning
}

// QueryResult allows access to the results of a query.
type QueryResult struct {
	reader analyticsRowReader

	unmarshaler Unmarshaler
}

// NextRow returns the next row in the result set, or nil if there are no more rows.
func (r *QueryResult) NextRow() *QueryResultRow {
	rowBytes := r.reader.NextRow()
	if rowBytes == nil {
		return nil
	}

	return &QueryResultRow{
		rowBytes:    rowBytes,
		unmarshaler: r.unmarshaler,
	}
}

// Err returns any errors that have occurred on the stream.
func (r *QueryResult) Err() error {
	if r.reader == nil {
		return ErrClosed
	}

	err := r.reader.Err()
	if err != nil {
		return err
	}

	return nil
}

// MetaData returns any meta-data that was available from this query.  Note that
// the meta-data will only be available once the object has been closed (either
// implicitly or explicitly).
func (r *QueryResult) MetaData() (*QueryMetadata, error) {
	meta, err := r.reader.MetaData()
	if err != nil {
		return nil, err
	}

	return meta, nil
}

// QueryResultRow encapsulates a single row of a query result.
type QueryResultRow struct {
	rowBytes []byte

	unmarshaler Unmarshaler
}

// ContentAs will attempt to unmarshal the content of the row into the provided value pointer.
func (qrr *QueryResultRow) ContentAs(valuePtr any) error {
	// We don't need to convert this error, if it's ours then we already have.
	// If it's the users then we don't want to interfere with it.
	return qrr.unmarshaler.Unmarshal(qrr.rowBytes, &valuePtr) // nolint:wrapcheck
}

// BufferQueryResult will buffer all rows in the result set into memory and return them as a slice, with any metadata.
func BufferQueryResult[T any](result *QueryResult) ([]T, *QueryMetadata, error) {
	if result == nil {
		return nil, nil, invalidArgumentError{
			ArgumentName: "result",
			Reason:       "result cannot be nil",
		}
	}

	var buffered []T

	for row := result.NextRow(); row != nil; row = result.NextRow() {
		var contentAs T

		err := row.ContentAs(&contentAs)
		if err != nil {
			return nil, nil, err
		}

		buffered = append(buffered, contentAs)
	}

	meta, err := result.MetaData()
	if err != nil {
		return nil, nil, err
	}

	err = result.Err()
	if err != nil {
		return nil, nil, err
	}

	return buffered, meta, nil
}

type analyticsRowReader interface {
	NextRow() []byte
	MetaData() (*QueryMetadata, error)
	Close() error
	Err() error
}

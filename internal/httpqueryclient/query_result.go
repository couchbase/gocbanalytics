package httpqueryclient

// QueryRowReader providers access to the rows of an analytics query
type QueryRowReader struct {
	streamer   *queryStreamer
	statement  string
	endpoint   string
	statusCode int
	peeked     []byte
}

// NextRow reads the next rows bytes from the stream
func (q *QueryRowReader) NextRow() []byte {
	if len(q.peeked) > 0 {
		peeked := q.peeked
		q.peeked = nil

		return peeked
	}

	return q.streamer.NextRow()
}

// Err returns any errors that occurred during streaming.
func (q *QueryRowReader) Err() error {
	err := q.streamer.Err()
	if err != nil {
		return err
	}

	meta, metaErr := q.streamer.MetaData()
	if metaErr != nil {
		return metaErr
	}

	cErr := parseAnalyticsErrorResponse(meta, q.statement, q.endpoint, q.statusCode, 0, "", 0)
	if cErr != nil {
		return cErr
	}

	return nil
}

// MetaData fetches the non-row bytes streamed in the response.
func (q *QueryRowReader) MetaData() ([]byte, error) {
	return q.streamer.MetaData()
}

// Close immediately shuts down the connection
func (q *QueryRowReader) Close() error {
	return q.streamer.Close()
}

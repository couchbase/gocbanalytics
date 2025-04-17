package cbcolumnar

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryErrorAsColumnarError(t *testing.T) {
	err := newQueryError("select *", "endpoint", 200, 23, "message")

	var columnarError *ColumnarError

	require.ErrorAs(t, err, &columnarError)
}

func TestQueryErrorIsErrQuery(t *testing.T) {
	err := newQueryError("select *", "endpoint", 200, 23, "message")

	require.ErrorIs(t, err, ErrQuery)
}

func TestQueryErrorAsQueryError(t *testing.T) {
	err := newQueryError("select *", "endpoint", 200, 23, "message")

	var queryError QueryError

	require.ErrorAs(t, err, &queryError)

	assert.Equal(t, 23, queryError.Code())
	assert.Equal(t, "message", queryError.Message())
}

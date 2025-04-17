package cbcolumnar_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	cbcolumnar "github.com/couchbase/gocbcolumnar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicQuery(t *testing.T) {
	cluster, err := cbcolumnar.NewCluster(TestOpts.OriginalConnStr, cbcolumnar.NewCredential(TestOpts.Username, TestOpts.Password), DefaultOptions())
	require.NoError(t, err)
	defer func(cluster *cbcolumnar.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	ExecuteQueryAgainst(t, []Queryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable Queryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		res, err := queryable.ExecuteQuery(ctx, "FROM RANGE(0, 99) AS i SELECT RAW i")
		require.NoError(tt, err)

		actualRows := CollectRows[int](t, res)
		require.Len(tt, actualRows, 100)

		for i := 0; i < 100; i++ {
			require.Equal(tt, i, actualRows[i])
		}

		err = res.Err()
		require.NoError(tt, err)

		meta, err := res.MetaData()
		require.NoError(tt, err)

		assertMeta(tt, meta, 100)
	})
}

func TestBasicBufferedQuery(t *testing.T) {
	cluster, err := cbcolumnar.NewCluster(TestOpts.OriginalConnStr, cbcolumnar.NewCredential(TestOpts.Username, TestOpts.Password), DefaultOptions())
	require.NoError(t, err)
	defer func(cluster *cbcolumnar.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	ExecuteQueryAgainst(t, []Queryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable Queryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		res, err := queryable.ExecuteQuery(ctx, "FROM RANGE(0, 99) AS i SELECT RAW i")
		require.NoError(tt, err)

		actualRows, meta, err := cbcolumnar.BufferQueryResult[int](res)
		require.NoError(tt, err)

		for i := 0; i < 100; i++ {
			require.Equal(tt, i, actualRows[i])
		}

		assertMeta(tt, meta, 100)
	})
}

func TestDispatchTimeout(t *testing.T) {
	// We're purposely using an invalid hostname so we need to suppress warnings.
	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	newCluster := func(tt *testing.T, dispatchTimeout time.Duration) *cbcolumnar.Cluster {
		cluster, err := cbcolumnar.NewCluster("couchbases://somenonsense?srv=false",
			cbcolumnar.NewCredential(TestOpts.Username, TestOpts.Password),
			DefaultOptions().SetTimeoutOptions(cbcolumnar.NewTimeoutOptions().SetQueryTimeout(dispatchTimeout)),
		)
		require.NoError(tt, err)

		return cluster
	}

	runTest := func(ctx context.Context, tt *testing.T, queryable Queryable, expectedErr error) {
		_, err := queryable.ExecuteQuery(ctx, "SELECT sleep('foo', 5000);")
		require.ErrorIs(tt, err, expectedErr)

		var columnarErr *cbcolumnar.ColumnarError

		require.ErrorAs(tt, err, &columnarErr)

		assert.Contains(tt, columnarErr.Error(), "operation not sent to server")
	}

	t.Run("Cluster Context Deadline", func(tt *testing.T) {
		cluster := newCluster(tt, 2*time.Second)
		defer func(cluster *cbcolumnar.Cluster) {
			err := cluster.Close()
			assert.NoError(t, err)
		}(cluster)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		runTest(ctx, tt, cluster, context.DeadlineExceeded)
	})

	t.Run("Scope Context Deadline", func(tt *testing.T) {
		cluster := newCluster(tt, 2*time.Second)
		defer func(cluster *cbcolumnar.Cluster) {
			err := cluster.Close()
			assert.NoError(t, err)
		}(cluster)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		runTest(ctx, tt, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope), context.DeadlineExceeded)
	})

	t.Run("Cluster Context Cancel", func(tt *testing.T) {
		cluster := newCluster(tt, 2*time.Second)
		defer func(cluster *cbcolumnar.Cluster) {
			err := cluster.Close()
			assert.NoError(t, err)
		}(cluster)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		cancel()

		runTest(ctx, tt, cluster, context.Canceled)
	})

	t.Run("Scope Context Cancel", func(tt *testing.T) {
		cluster := newCluster(tt, 2*time.Second)
		defer func(cluster *cbcolumnar.Cluster) {
			err := cluster.Close()
			assert.NoError(t, err)
		}(cluster)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		cancel()

		runTest(ctx, tt, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope), context.Canceled)
	})

	t.Run("Cluster Timeout", func(tt *testing.T) {
		cluster := newCluster(tt, 1*time.Second)
		defer func(cluster *cbcolumnar.Cluster) {
			err := cluster.Close()
			assert.NoError(t, err)
		}(cluster)

		runTest(context.Background(), tt, cluster, cbcolumnar.ErrTimeout)
	})

	t.Run("Scope Timeout", func(tt *testing.T) {
		cluster := newCluster(tt, 1*time.Second)
		defer func(cluster *cbcolumnar.Cluster) {
			err := cluster.Close()
			assert.NoError(t, err)
		}(cluster)

		runTest(context.Background(), tt, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope), cbcolumnar.ErrTimeout)
	})
}

func TestOperationTimeout(t *testing.T) {
	cluster, err := cbcolumnar.NewCluster(TestOpts.OriginalConnStr,
		cbcolumnar.NewCredential(TestOpts.Username, TestOpts.Password),
		DefaultOptions(),
	)
	require.NoError(t, err)
	defer func(cluster *cbcolumnar.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	t.Run("Context Deadline", func(tt *testing.T) {
		ExecuteQueryAgainst(tt, []Queryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(ttt *testing.T, queryable Queryable) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			_, err := queryable.ExecuteQuery(ctx, "SELECT sleep('foo', 5000);")
			require.ErrorIs(ttt, err, context.DeadlineExceeded)

			var columnarErr *cbcolumnar.ColumnarError

			require.ErrorAs(ttt, err, &columnarErr)

			assert.NotContains(ttt, columnarErr.Error(), "operation not sent to server")
		})
	})

	t.Run("Context Cancel", func(tt *testing.T) {
		ExecuteQueryAgainst(tt, []Queryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(ttt *testing.T, queryable Queryable) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			go func() {
				time.Sleep(1 * time.Second)
				cancel()
			}()

			_, err := queryable.ExecuteQuery(ctx, "SELECT sleep('foo', 5000);")
			require.ErrorIs(ttt, err, context.Canceled)

			var columnarErr *cbcolumnar.ColumnarError

			require.ErrorAs(ttt, err, &columnarErr)

			assert.NotContains(ttt, columnarErr.Error(), "operation not sent to server")
		})
	})

	t.Run("Timeout", func(tt *testing.T) {
		cluster, err := cbcolumnar.NewCluster(TestOpts.OriginalConnStr,
			cbcolumnar.NewCredential(TestOpts.Username, TestOpts.Password),
			DefaultOptions().SetTimeoutOptions(cbcolumnar.NewTimeoutOptions().SetQueryTimeout(1*time.Second)),
		)
		require.NoError(tt, err)
		defer func(cluster *cbcolumnar.Cluster) {
			err := cluster.Close()
			assert.NoError(tt, err)
		}(cluster)

		ExecuteQueryAgainst(tt, []Queryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(ttt *testing.T, queryable Queryable) {
			ctx := context.Background()

			_, err := queryable.ExecuteQuery(ctx, "SELECT sleep('foo', 5000);")
			require.ErrorIs(ttt, err, cbcolumnar.ErrTimeout)

			var columnarErr *cbcolumnar.ColumnarError

			require.ErrorAs(ttt, err, &columnarErr)

			assert.NotContains(ttt, columnarErr.Error(), "operation not sent to server")
		})
	})
}

func TestQueryError(t *testing.T) {
	cluster, err := cbcolumnar.NewCluster(TestOpts.OriginalConnStr,
		cbcolumnar.NewCredential(TestOpts.Username, TestOpts.Password),
		DefaultOptions(),
	)
	require.NoError(t, err)
	defer func(cluster *cbcolumnar.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	ExecuteQueryAgainst(t, []Queryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable Queryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, err := queryable.ExecuteQuery(ctx, "SELEC 123;")
		require.ErrorIs(tt, err, cbcolumnar.ErrQuery)

		var columnarErr *cbcolumnar.ColumnarError

		require.ErrorAs(tt, err, &columnarErr)

		var queryErr *cbcolumnar.QueryError

		require.ErrorAs(tt, err, &queryErr)

		assert.Equal(tt, 24000, queryErr.Code())
		assert.NotEmpty(tt, queryErr.Message())
	})
}

func TestUnmarshaler(t *testing.T) {
	unmarshaler := &ErrorUnmarshaler{
		Err: errors.New("something went wrong"), // nolint: err113
	}

	cluster, err := cbcolumnar.NewCluster(TestOpts.OriginalConnStr,
		cbcolumnar.NewCredential(TestOpts.Username, TestOpts.Password),
		DefaultOptions().SetUnmarshaler(unmarshaler),
	)
	require.NoError(t, err)
	defer func(cluster *cbcolumnar.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	ExecuteQueryAgainst(t, []Queryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable Queryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		res, err := queryable.ExecuteQuery(ctx, "FROM RANGE(0, 1) AS i SELECT RAW i")
		require.NoError(tt, err)

		row := res.NextRow()
		require.NotNil(tt, row)

		var val interface{}
		err = row.ContentAs(&val)
		require.ErrorIs(tt, err, unmarshaler.Err)
	})
}

type ErrorUnmarshaler struct {
	Err error
}

func (e *ErrorUnmarshaler) Unmarshal(_ []byte, _ interface{}) error {
	return e.Err
}

func assertMeta(t *testing.T, meta *cbcolumnar.QueryMetadata, resultCount uint64) {
	assert.Empty(t, meta.Warnings)
	assert.NotEmpty(t, meta.RequestID)

	assert.NotZero(t, meta.Metrics.ElapsedTime)
	assert.NotZero(t, meta.Metrics.ExecutionTime)
	assert.NotZero(t, meta.Metrics.ResultSize)
	assert.Equal(t, resultCount, meta.Metrics.ResultCount)
	assert.Zero(t, meta.Metrics.ProcessedObjects)
}

type Queryable interface {
	ExecuteQuery(ctx context.Context, statement string, opts ...*cbcolumnar.QueryOptions) (*cbcolumnar.QueryResult, error)
}

func ExecuteQueryAgainst(t *testing.T, queryables []Queryable, fn func(tt *testing.T, queryable Queryable)) {
	for _, queryable := range queryables {
		t.Run(reflect.TypeOf(queryable).Elem().String(), func(tt *testing.T) {
			fn(tt, queryable)
		})
	}
}

func CollectRows[T any](t *testing.T, res *cbcolumnar.QueryResult) []T {
	var actualRows []T

	for row := res.NextRow(); row != nil; row = res.NextRow() {
		var actualRow T
		err := row.ContentAs(&actualRow)
		require.NoError(t, err)

		actualRows = append(actualRows, actualRow)
	}

	return actualRows
}

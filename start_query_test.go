package cbanalytics_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	cbanalytics "github.com/couchbase/gocbanalytics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type DeferredQueryable interface {
	StartQuery(ctx context.Context, statement string, opts ...*cbanalytics.StartQueryOptions) (*cbanalytics.QueryHandle, error)
}

func StartQueryAgainst(t *testing.T, queryables []DeferredQueryable, fn func(tt *testing.T, queryable DeferredQueryable)) {
	for _, queryable := range queryables {
		t.Run(reflect.TypeOf(queryable).Elem().String(), func(tt *testing.T) {
			fn(tt, queryable)
		})
	}
}

func TestStartQueryGoldenPath(t *testing.T) {
	cluster, err := cbanalytics.NewCluster(TestOpts.OriginalConnStr, cbanalytics.NewBasicAuthCredential(TestOpts.Username, TestOpts.Password), DefaultOptions())
	require.NoError(t, err)
	defer func(cluster *cbanalytics.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	StartQueryAgainst(t, []DeferredQueryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable DeferredQueryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		handle, err := queryable.StartQuery(ctx, "FROM RANGE(0, 99) AS i SELECT RAW i")
		require.NoError(tt, err)
		require.NotNil(tt, handle)

		var resultHandle *cbanalytics.QueryResultHandle

		require.Eventually(tt, func() bool {
			rh, done, fetchErr := handle.FetchResultHandle(ctx)
			if fetchErr != nil {
				tt.Logf("FetchResultHandle error: %v", fetchErr)

				return false
			}

			if done {
				resultHandle = rh
			}

			return done
		}, 60*time.Second, 500*time.Millisecond, "query did not complete in time")

		require.NotNil(tt, resultHandle)

		res, err := resultHandle.FetchResults(ctx)
		require.NoError(tt, err)

		actualRows := CollectRows[int](tt, res)
		require.Len(tt, actualRows, 100)

		for i := 0; i < 100; i++ {
			require.Equal(tt, i, actualRows[i])
		}

		err = res.Err()
		require.NoError(tt, err)
	})
}

func TestStartQueryCancel(t *testing.T) {
	cluster, err := cbanalytics.NewCluster(TestOpts.OriginalConnStr, cbanalytics.NewBasicAuthCredential(TestOpts.Username, TestOpts.Password), DefaultOptions())
	require.NoError(t, err)
	defer func(cluster *cbanalytics.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	StartQueryAgainst(t, []DeferredQueryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable DeferredQueryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		handle, err := queryable.StartQuery(ctx, "SELECT sleep('foo', 30000)")
		require.NoError(tt, err)
		require.NotNil(tt, handle)

		err = handle.Cancel(ctx)
		require.NoError(tt, err)
	})
}

func TestStartQueryDiscardResults(t *testing.T) {
	cluster, err := cbanalytics.NewCluster(TestOpts.OriginalConnStr, cbanalytics.NewBasicAuthCredential(TestOpts.Username, TestOpts.Password), DefaultOptions())
	require.NoError(t, err)
	defer func(cluster *cbanalytics.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	StartQueryAgainst(t, []DeferredQueryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable DeferredQueryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		handle, err := queryable.StartQuery(ctx, "FROM RANGE(0, 9) AS i SELECT RAW i")
		require.NoError(tt, err)
		require.NotNil(tt, handle)

		var resultHandle *cbanalytics.QueryResultHandle

		require.Eventually(tt, func() bool {
			rh, done, fetchErr := handle.FetchResultHandle(ctx)
			if fetchErr != nil {
				tt.Logf("FetchResultHandle error: %v", fetchErr)

				return false
			}

			if done {
				resultHandle = rh
			}

			return done
		}, 60*time.Second, 500*time.Millisecond, "query did not complete in time")

		require.NotNil(tt, resultHandle)

		err = resultHandle.DiscardResults(ctx)
		require.NoError(tt, err)
	})
}

// TestFetchResult_DefaultUnmarshaler verifies that FetchResults uses the default unmarshaler
// when no FetchResultsOptions are provided.
func TestFetchResult_DefaultUnmarshaler(t *testing.T) {
	cluster, err := cbanalytics.NewCluster(TestOpts.OriginalConnStr, cbanalytics.NewBasicAuthCredential(TestOpts.Username, TestOpts.Password), DefaultOptions())
	require.NoError(t, err)
	defer func(cluster *cbanalytics.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	StartQueryAgainst(t, []DeferredQueryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable DeferredQueryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		handle, err := queryable.StartQuery(ctx, "FROM RANGE(0, 9) AS i SELECT RAW i")
		require.NoError(tt, err)
		require.NotNil(tt, handle)

		var resultHandle *cbanalytics.QueryResultHandle

		require.Eventually(tt, func() bool {
			rh, done, fetchErr := handle.FetchResultHandle(ctx)
			if fetchErr != nil {
				tt.Logf("FetchResultHandle error: %v", fetchErr)

				return false
			}

			if done {
				resultHandle = rh
			}

			return done
		}, 60*time.Second, 500*time.Millisecond, "query did not complete in time")

		require.NotNil(tt, resultHandle)

		// Call FetchResults without any options; the default JSON unmarshaler should be used.
		res, err := resultHandle.FetchResults(ctx)
		require.NoError(tt, err)

		actualRows := CollectRows[int](tt, res)
		require.Len(tt, actualRows, 10)

		for i := 0; i < 10; i++ {
			require.Equal(tt, i, actualRows[i])
		}

		err = res.Err()
		require.NoError(tt, err)
	})
}

// TestFetchResult_CustomUnmarshaler verifies that FetchResults uses a custom unmarshaler
// provided via FetchResultsOptions, overriding the default.
func TestFetchResult_CustomUnmarshaler(t *testing.T) {
	cluster, err := cbanalytics.NewCluster(TestOpts.OriginalConnStr, cbanalytics.NewBasicAuthCredential(TestOpts.Username, TestOpts.Password), DefaultOptions())
	require.NoError(t, err)
	defer func(cluster *cbanalytics.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	StartQueryAgainst(t, []DeferredQueryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable DeferredQueryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		handle, err := queryable.StartQuery(ctx, "FROM RANGE(0, 9) AS i SELECT RAW i")
		require.NoError(tt, err)
		require.NotNil(tt, handle)

		var resultHandle *cbanalytics.QueryResultHandle

		require.Eventually(tt, func() bool {
			rh, done, fetchErr := handle.FetchResultHandle(ctx)
			if fetchErr != nil {
				tt.Logf("FetchResultHandle error: %v", fetchErr)

				return false
			}

			if done {
				resultHandle = rh
			}

			return done
		}, 60*time.Second, 500*time.Millisecond, "query did not complete in time")

		require.NotNil(tt, resultHandle)

		// Call FetchResults with a custom unmarshaler that always returns an error.
		customErr := errors.New("custom unmarshal error") //nolint:err113
		res, err := resultHandle.FetchResults(ctx,
			cbanalytics.NewFetchResultOptions().SetUnmarshaler(&ErrorUnmarshaler{Err: customErr}),
		)
		require.NoError(tt, err)

		// Every row should fail to decode with the custom error.
		for row := res.NextRow(); row != nil; row = res.NextRow() {
			var val interface{}

			err = row.ContentAs(&val)
			require.ErrorIs(tt, err, customErr)
		}

		err = res.Err()
		require.NoError(tt, err)
	})
}

func TestStartQueryError(t *testing.T) {
	cluster, err := cbanalytics.NewCluster(TestOpts.OriginalConnStr,
		cbanalytics.NewBasicAuthCredential(TestOpts.Username, TestOpts.Password),
		DefaultOptions(),
	)
	require.NoError(t, err)
	defer func(cluster *cbanalytics.Cluster) {
		err := cluster.Close()
		assert.NoError(t, err)
	}(cluster)

	StartQueryAgainst(t, []DeferredQueryable{cluster, cluster.Database(TestOpts.Database).Scope(TestOpts.Scope)}, func(tt *testing.T, queryable DeferredQueryable) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, err := queryable.StartQuery(ctx, "SELEC 123;")
		require.ErrorIs(tt, err, cbanalytics.ErrQuery)

		var analyticsErr *cbanalytics.AnalyticsError

		require.ErrorAs(tt, err, &analyticsErr)

		var queryErr *cbanalytics.QueryError

		require.ErrorAs(tt, err, &queryErr)

		assert.Equal(tt, 24000, queryErr.Code())
		assert.NotEmpty(tt, queryErr.Message())
	})
}

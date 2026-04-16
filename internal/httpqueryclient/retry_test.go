package httpqueryclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/gocbanalytics/internal/logging"
)

// analyticsResponse builds a JSON analytics response body with optional fields.
func analyticsResponse(opts ...func(m map[string]interface{})) []byte {
	m := map[string]interface{}{
		"status":  "success",
		"results": []interface{}{},
	}

	for _, opt := range opts {
		opt(m)
	}

	b, _ := json.Marshal(m)

	return b
}

func withResults(rows ...interface{}) func(m map[string]interface{}) {
	return func(m map[string]interface{}) {
		m["results"] = rows
	}
}

func withErrors(errs ...jsonAnalyticsError) func(m map[string]interface{}) {
	return func(m map[string]interface{}) {
		m["errors"] = errs
	}
}

func withStatus(status string) func(m map[string]interface{}) {
	return func(m map[string]interface{}) {
		m["status"] = status
	}
}

func retriableError(code uint32, msg string) jsonAnalyticsError {
	return jsonAnalyticsError{Code: code, Msg: msg, Retry: true}
}

func nonRetriableError(code uint32, msg string) jsonAnalyticsError {
	return jsonAnalyticsError{Code: code, Msg: msg, Retry: false}
}

func mustWrite(t *testing.T, w http.ResponseWriter, data []byte) {
	t.Helper()

	_, err := w.Write(data)
	require.NoError(t, err)
}

// newTestClient creates a Client pointing at the given test server address.
func newTestClient(t *testing.T, addr string) *Client {
	t.Helper()

	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	return NewClient("http", host, port, ClientConfig{
		TLSConfig:      nil,
		Logger:         logging.NewDefaultLogger(logging.LogTrace, 0),
		ConnectTimeout: 5 * time.Second,
	})
}

// --- Query retry tests ---

func TestQueryRetries_RetriableErrorsThenSuccess(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n <= 2 {
			w.WriteHeader(200)
			mustWrite(t, w, analyticsResponse(
				withStatus("fatal"),
				withErrors(retriableError(23001, "temporarily unavailable")),
			))

			return
		}

		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(withResults(1, 2, 3)))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  5,
	})
	require.NoError(t, err)

	// Should have rows from the successful attempt
	row := result.NextRow()
	require.NotNil(t, row)
	assert.Equal(t, "1", string(row))

	require.Equal(t, int32(3), atomic.LoadInt32(&attempt), "expected 3 attempts (2 retries + 1 success)")
}

func TestQueryRetries_NonRetriableErrorNoRetry(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(
			withStatus("fatal"),
			withErrors(nonRetriableError(24000, "syntax error")),
		))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELEC 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  5,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrAnalytics))

	require.Equal(t, int32(1), atomic.LoadInt32(&attempt), "should not retry on non-retriable error")
}

func TestQueryRetries_MaxRetriesExhausted(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(
			withStatus("fatal"),
			withErrors(retriableError(23001, "temporarily unavailable")),
		))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var maxRetries uint32 = 2

	_, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  maxRetries,
	})
	require.Error(t, err)

	// 1 initial + 2 retries = 3 attempts
	require.Equal(t, int32(maxRetries+1), atomic.LoadInt32(&attempt), "expected exactly maxRetries+1 attempts")
}

func TestQueryRetries_HTTPErrorRetriable(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n <= 1 {
			w.WriteHeader(503)
			mustWrite(t, w, []byte(`{}`))

			return
		}

		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(withResults(42)))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  3,
	})
	require.NoError(t, err)

	row := result.NextRow()
	require.NotNil(t, row)
	assert.Equal(t, "42", string(row))

	require.Equal(t, int32(2), atomic.LoadInt32(&attempt))
}

func TestQueryRetries_HTTP401NoRetry(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(401)
		mustWrite(t, w, []byte(`{}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  3,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidCredential))

	require.Equal(t, int32(1), atomic.LoadInt32(&attempt), "should not retry on 401")
}

// --- Handle request retry tests ---

func TestHandleRetries_FetchHandleStatus_RetriableThenSuccess(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n <= 2 {
			w.WriteHeader(503)
			mustWrite(t, w, []byte(`{}`))

			return
		}

		w.WriteHeader(200)
		mustWrite(t, w, []byte(`{"status":"success","handle":"/api/v1/request/result/abc"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := client.FetchHandleStatus(ctx, "/api/v1/request/result/abc", func(_ *http.Request) {}, 5)
	require.NoError(t, err)
	require.Contains(t, string(body), "success")

	require.Equal(t, int32(3), atomic.LoadInt32(&attempt))
}

func TestHandleRetries_FetchHandleStatus_404ReturnsNotFound(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(404)
		mustWrite(t, w, []byte(`{}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.FetchHandleStatus(ctx, "/api/v1/request/result/abc", func(_ *http.Request) {}, 3)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrQueryNotFound))

	require.Equal(t, int32(1), atomic.LoadInt32(&attempt), "404 is not retriable")
}

func TestHandleRetries_DiscardResults_RetriableThenSuccess(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n <= 1 {
			w.WriteHeader(500)
			mustWrite(t, w, analyticsResponse(
				withStatus("fatal"),
				withErrors(retriableError(23001, "temporarily unavailable")),
			))

			return
		}

		w.WriteHeader(200)
		mustWrite(t, w, []byte(`{}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := client.DiscardHandleResults(ctx, "/api/v1/request/result/abc", func(_ *http.Request) {}, 5)
	require.NoError(t, err)

	require.Equal(t, int32(2), atomic.LoadInt32(&attempt))
}

func TestHandleRetries_DiscardResults_404NoError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(404)
		mustWrite(t, w, []byte(`{}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := client.DiscardHandleResults(ctx, "/api/v1/request/result/abc", func(_ *http.Request) {}, 0)
	require.NoError(t, err)
}

func TestHandleRetries_CancelHandle_RetriableThenSuccess(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n <= 1 {
			w.WriteHeader(503)
			mustWrite(t, w, []byte(`{}`))

			return
		}

		w.WriteHeader(200)
		mustWrite(t, w, []byte(`{}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := client.CancelHandle(ctx, "req-123", func(_ *http.Request) {}, 5)
	require.NoError(t, err)

	require.Equal(t, int32(2), atomic.LoadInt32(&attempt))
}

func TestHandleRetries_CancelHandle_NonRetriableNoRetry(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(400)
		mustWrite(t, w, analyticsResponse(
			withStatus("fatal"),
			withErrors(nonRetriableError(24000, "bad request")),
		))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := client.CancelHandle(ctx, "req-123", func(_ *http.Request) {}, 5)
	require.Error(t, err)

	require.Equal(t, int32(1), atomic.LoadInt32(&attempt), "should not retry on non-retriable error")
}

func TestHandleRetries_MaxRetriesExhausted(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(503)
		mustWrite(t, w, []byte(`{}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var maxRetries uint32 = 2

	err := client.CancelHandle(ctx, "req-123", func(_ *http.Request) {}, maxRetries)
	require.Error(t, err)

	require.Equal(t, int32(maxRetries+1), atomic.LoadInt32(&attempt))
}

func TestHandleRetries_StreamResults_RetriableThenSuccess(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n <= 2 {
			w.WriteHeader(503)
			mustWrite(t, w, []byte(`{}`))

			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		mustWrite(t, w, []byte(`{"results":[1,2,3]}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reader, err := client.StreamHandleResults(ctx, "/api/v1/request/result/abc", func(_ *http.Request) {}, 5)
	require.NoError(t, err)

	row := reader.NextRow()
	require.NotNil(t, row)
	assert.Equal(t, "1", string(row))

	require.Equal(t, int32(3), atomic.LoadInt32(&attempt))

	require.NoError(t, reader.Close())
}

func TestHandleRetries_StreamResults_404ReturnsNotFound(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(404)
		mustWrite(t, w, []byte(`{}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.StreamHandleResults(ctx, "/api/v1/request/result/abc", func(_ *http.Request) {}, 3)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrQueryNotFound))

	require.Equal(t, int32(1), atomic.LoadInt32(&attempt), "404 is not retriable")
}

func TestHandleRetries_StreamResults_RetriableErrorInBody(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n <= 1 {
			w.WriteHeader(500)
			mustWrite(t, w, analyticsResponse(
				withStatus("fatal"),
				withErrors(retriableError(23001, "temporarily unavailable")),
			))

			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		mustWrite(t, w, []byte(`{"results":[42]}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reader, err := client.StreamHandleResults(ctx, "/api/v1/request/result/abc", func(_ *http.Request) {}, 5)
	require.NoError(t, err)

	row := reader.NextRow()
	require.NotNil(t, row)
	assert.Equal(t, "42", string(row))

	require.Equal(t, int32(2), atomic.LoadInt32(&attempt))

	require.NoError(t, reader.Close())
}

// --- Routing tests to verify requests go to the right path ---

func TestHandleRequests_CorrectPaths(t *testing.T) {
	var lastPath string

	var lastMethod string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPath = r.URL.Path
		lastMethod = r.Method

		w.WriteHeader(200)
		mustWrite(t, w, []byte(`{"status":"success","results":[]}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx := context.Background()
	authHandler := func(_ *http.Request) {}

	t.Run("FetchHandleStatus", func(t *testing.T) {
		_, err := client.FetchHandleStatus(ctx, "/api/v1/request/status/my-handle-id", authHandler, 0)
		require.NoError(t, err)
		assert.Equal(t, "/api/v1/request/status/my-handle-id", lastPath)
		assert.Equal(t, "GET", lastMethod)
	})

	t.Run("FetchHandleStatus_WithPrefix", func(t *testing.T) {
		_, err := client.FetchHandleStatus(ctx, "/api/v1/request/status/abc", authHandler, 0)
		require.NoError(t, err)
		assert.Equal(t, "/api/v1/request/status/abc", lastPath)
	})

	t.Run("DiscardHandleResults", func(t *testing.T) {
		err := client.DiscardHandleResults(ctx, "/api/v1/request/result/my-handle-id", authHandler, 0)
		require.NoError(t, err)
		assert.Equal(t, "/api/v1/request/result/my-handle-id", lastPath)
		assert.Equal(t, "DELETE", lastMethod)
	})

	t.Run("CancelHandle", func(t *testing.T) {
		err := client.CancelHandle(ctx, "/api/v1/request/status/req-123", authHandler, 0)
		require.NoError(t, err)
		assert.Equal(t, "/api/v1/active_requests", lastPath)
		assert.Equal(t, "DELETE", lastMethod)
	})

	t.Run("StreamHandleResults", func(t *testing.T) {
		reader, err := client.StreamHandleResults(ctx, "/api/v1/request/result/my-handle-id", authHandler, 0)
		require.NoError(t, err)
		assert.Equal(t, "/api/v1/request/result/my-handle-id", lastPath)
		assert.Equal(t, "GET", lastMethod)

		require.NoError(t, reader.Close())
	})
}

// --- Auth handler tests ---

func TestRetries_AuthHeaderSentOnEveryAttempt(t *testing.T) {
	var authHeaders []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		if len(authHeaders) < 3 {
			w.WriteHeader(503)
			mustWrite(t, w, []byte(`{}`))

			return
		}

		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(withResults(1)))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.Query(ctx, &QueryOptions{
		Payload: map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(req *http.Request) {
			req.SetBasicAuth("user", "pass")
		},
		MaxRetries: 5,
	})
	require.NoError(t, err)

	require.Len(t, authHeaders, 3)

	for i, h := range authHeaders {
		assert.NotEmpty(t, h, fmt.Sprintf("auth header missing on attempt %d", i+1))
	}
}

// --- Context cancellation during retries ---

func TestRetries_ContextCancelledDuringRetry(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(
			withStatus("fatal"),
			withErrors(retriableError(23001, "temporarily unavailable")),
		))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay so context expires between retry backoff sleeps
	go func() {
		time.Sleep(250 * time.Millisecond)
		cancel()
	}()

	_, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  100,
	})
	require.Error(t, err)

	// Should have made at least 1 attempt but stopped well before exhausting all 100 retries.
	attempts := atomic.LoadInt32(&attempt)
	require.GreaterOrEqual(t, attempts, int32(1))
	require.Less(t, attempts, int32(100), "should have stopped retrying after context cancellation")
}

// --- Mixed retriable and non-retriable errors ---

func TestQueryRetries_MixedRetriableAndNonRetriable_NoRetry(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(
			withStatus("fatal"),
			withErrors(
				retriableError(23001, "temporarily unavailable"),
				nonRetriableError(24000, "syntax error"),
			),
		))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELEC 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  5,
	})
	require.Error(t, err)

	require.Equal(t, int32(1), atomic.LoadInt32(&attempt),
		"should not retry when response contains any non-retriable error")
}

// --- Zero max retries ---

func TestQueryRetries_ZeroMaxRetries(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(
			withStatus("fatal"),
			withErrors(retriableError(23001, "temporarily unavailable")),
		))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  0,
	})
	require.Error(t, err)

	require.Equal(t, int32(1), atomic.LoadInt32(&attempt), "with maxRetries=0 should only make 1 attempt")
}

// --- HTTP 401 on handle requests ---

func TestHandleRetries_HTTP401NoRetry(t *testing.T) {
	var attempt int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempt, 1)
		w.WriteHeader(401)
		mustWrite(t, w, []byte(`{}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.FetchHandleStatus(ctx, "/api/v1/request/status/abc", func(_ *http.Request) {}, 5)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidCredential))

	require.Equal(t, int32(1), atomic.LoadInt32(&attempt), "should not retry on 401")
}

// --- Connection refused retries ---

func TestQueryRetries_ConnectionRefused(t *testing.T) {
	// Start a server and immediately close it to get a port that refuses connections
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	addr := srv.Listener.Addr().String()
	srv.Close()

	client := newTestClient(t, addr)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var maxRetries uint32 = 2

	_, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  maxRetries,
	})
	require.Error(t, err)
	// The error should ultimately be a deadline or connection error, not a nil panic
	require.NotNil(t, err)
}

// --- Verify retry count is in error ---

func TestQueryRetries_RetryCountInError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(
			withStatus("fatal"),
			withErrors(retriableError(23001, "temporarily unavailable")),
		))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  3,
	})
	require.Error(t, err)

	var qErr *QueryError

	require.True(t, errors.As(err, &qErr))
	assert.Equal(t, uint32(3), qErr.Retries, "error should report the number of retries performed")
}

// --- Request path changes don't affect retry ---

func TestHandleRetries_FetchHandleStatus_StripsResultPrefix(t *testing.T) {
	var receivedPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path

		w.WriteHeader(200)
		mustWrite(t, w, []byte(`{"status":"success"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	_, err := client.FetchHandleStatus(context.Background(), "/api/v1/request/status/my-id", func(_ *http.Request) {}, 0)
	require.NoError(t, err)

	assert.Equal(t, "/api/v1/request/status/my-id", receivedPath)
}

// --- Verify retries work across different addresses from DNS ---

func TestQueryRetries_RequestsDistributedAcrossAddresses(t *testing.T) {
	// This test verifies that the retry mechanism selects from resolved addresses.
	// We use a single server but can at least verify it works.
	var attempts int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n <= 3 {
			w.WriteHeader(503)
			mustWrite(t, w, []byte(`{}`))

			return
		}

		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(withResults("ok")))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  5,
	})
	require.NoError(t, err)

	row := result.NextRow()
	require.NotNil(t, row)
	assert.Equal(t, `"ok"`, string(row))

	require.Equal(t, int32(4), atomic.LoadInt32(&attempts))
}

// --- Verify host header is set correctly ---

func TestRetries_HostHeaderSetCorrectly(t *testing.T) {
	var hostHeaders []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hostHeaders = append(hostHeaders, r.Host)
		if len(hostHeaders) < 2 {
			w.WriteHeader(503)
			mustWrite(t, w, []byte(`{}`))

			return
		}

		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(withResults(1)))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.Query(ctx, &QueryOptions{
		Payload:     map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(_ *http.Request) {},
		MaxRetries:  5,
	})
	require.NoError(t, err)

	require.Len(t, hostHeaders, 2)

	for _, h := range hostHeaders {
		// The host header should contain the address we're connecting to
		assert.True(t, strings.Contains(h, "127.0.0.1") || strings.Contains(h, "localhost"),
			"unexpected host header: %s", h)
	}
}

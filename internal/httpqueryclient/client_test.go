package httpqueryclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientDoesNotFollowRedirects(t *testing.T) {
	var targetRequests int32

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&targetRequests, 1)
		w.WriteHeader(200)
		mustWrite(t, w, analyticsResponse(withResults(1)))
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/api/v1/request", http.StatusFound)
	}))
	defer source.Close()

	client := newTestClient(t, source.Listener.Addr().String())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.Query(ctx, &QueryOptions{
		Payload: map[string]interface{}{"statement": "SELECT 1"},
		AuthHandler: func(req *http.Request) {
			req.SetBasicAuth("user", "pass")
		},
		MaxRetries: 5,
	})
	require.Error(t, err)

	var qErr *QueryError
	require.ErrorAs(t, err, &qErr)
	assert.Equal(t, http.StatusFound, qErr.HTTPResponseCode)

	assert.Equal(t, int32(0), atomic.LoadInt32(&targetRequests), "redirect target should not be contacted")
}

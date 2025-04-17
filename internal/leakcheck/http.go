package leakcheck

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

var leakTrackingEnabled uint32
var trackedRespsLock sync.Mutex
var trackedResps []*leakTrackingReadCloser

// EnableHTTPResponseTracking enables tracking response bodies to ensure that they are
// eventually closed.
func EnableHTTPResponseTracking() {
	atomic.StoreUint32(&leakTrackingEnabled, 1)
}

// WrapHTTPResponse wraps an HTTP response body to track it for leaks.
func WrapHTTPResponse(resp *http.Response) *http.Response {
	if atomic.LoadUint32(&leakTrackingEnabled) == 0 {
		return resp
	}

	trackingBody := &leakTrackingReadCloser{
		parent:     resp.Body,
		stackTrace: debug.Stack(),
	}

	trackedRespsLock.Lock()
	trackedResps = append(trackedResps, trackingBody)
	trackedRespsLock.Unlock()

	resp.Body = trackingBody

	return resp
}

func removeTrackedHTTPBodyRecord(l *leakTrackingReadCloser) {
	trackedRespsLock.Lock()
	recordIdx := -1

	for i, tracked := range trackedResps {
		if tracked == l {
			recordIdx = i
		}
	}

	if recordIdx >= 0 {
		trackedResps = append(trackedResps[:recordIdx], trackedResps[recordIdx+1:]...)
	}
	trackedRespsLock.Unlock()
}

// ReportLeakedHTTPResponses prints the stack traces of any response bodies that have not
// been closed. Returns true if all bodies have been closed, false otherwise.
func ReportLeakedHTTPResponses() bool {
	if len(trackedResps) == 0 {
		log.Printf("No leaked http requests")

		return true
	}

	log.Printf("Found %d leaked http requests", len(trackedResps))

	for _, leakRecord := range trackedResps {
		log.Printf("Leaked http request stack: %s", leakRecord.stackTrace)
	}

	return false
}

type leakTrackingReadCloser struct {
	parent     io.ReadCloser
	stackTrace []byte
}

func (l *leakTrackingReadCloser) Read(p []byte) (int, error) {
	n, err := l.parent.Read(p)
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		removeTrackedHTTPBodyRecord(l)
	}

	return n, err
}

func (l *leakTrackingReadCloser) Close() error {
	removeTrackedHTTPBodyRecord(l)

	return l.parent.Close()
}

var _ io.ReadCloser = (*leakTrackingReadCloser)(nil)

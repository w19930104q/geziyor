package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chromedp/chromedp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testReplayMemLimit = 1024       // M: 1KB, forces file spill for larger bodies
	testReplayMaxSize  = 1024 * 256 // L: 256KB
)

// newReplayClient creates a client with body replay enabled and small limits.
func newReplayClient() *Client {
	return NewClient(&Options{
		MaxBodySize:           DefaultMaxBody,
		RetryTimes:            DefaultRetryTimes,
		RetryHTTPCodes:        DefaultRetryHTTPCodes,
		AllocatorOptions:      chromedp.DefaultExecAllocatorOptions[:],
		BodyReplay:            true,
		BodyReplayMemoryLimit: testReplayMemLimit,
		BodyReplayMaxSize:     testReplayMaxSize,
	})
}

// closeCounter wraps a reader and counts Close calls.
type closeCounter struct {
	io.Reader
	closes *int32
}

func (c closeCounter) Close() error {
	atomic.AddInt32(c.closes, 1)
	return nil
}

// failReader returns an error after some bytes.
type failReader struct {
	r     io.Reader
	limit int
	read  int
}

func (f *failReader) Read(p []byte) (int, error) {
	if f.read >= f.limit {
		return 0, errors.New("boom")
	}
	n, err := f.r.Read(p)
	f.read += n
	return n, err
}

func (f *failReader) Close() error { return nil }

// digestServer records the sha256 digest of each received request body.
type digestServer struct {
	t       *testing.T
	digests []string
	count   int
}

func (d *digestServer) handler(statusCodes []int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		d.digests = append(d.digests, fmt.Sprintf("%x", sha256.Sum256(body)))
		d.count++
		status := http.StatusOK
		if d.count <= len(statusCodes) {
			status = statusCodes[d.count-1]
		}
		w.WriteHeader(status)
	}
}

// TestBodyReplayRetryReplaysBody asserts that every retry sends the full
// original body: all attempt digests are identical and the retry count limit
// is respected. The body is larger than M to force temp file spillover.
func TestBodyReplayRetryReplaysBody(t *testing.T) {
	d := &digestServer{t: t}
	ts := httptest.NewServer(d.handler([]int{500, 502, 200}))
	defer ts.Close()

	var closes int32
	body := strings.Repeat("geziyor-replay-", 10000) // ~150KB > M
	req, err := NewRequest("POST", ts.URL, nil)
	require.NoError(t, err)
	req.Body = closeCounter{strings.NewReader(body), &closes}
	req.ContentLength = int64(len(body))

	res, err := newReplayClient().DoRequest(req)
	require.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)

	// 1 initial + 2 retries (DefaultRetryTimes), each with identical body.
	assert.Equal(t, 3, d.count)
	require.Len(t, d.digests, 3)
	assert.Equal(t, d.digests[0], d.digests[1])
	assert.Equal(t, d.digests[0], d.digests[2])
	expected := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
	assert.Equal(t, expected, d.digests[0])

	// Original body closed exactly once.
	assert.Equal(t, int32(1), atomic.LoadInt32(&closes))
}

// TestBodyReplayRedirect307PreservesBody asserts 307 redirects replay the body.
func TestBodyReplayRedirect307PreservesBody(t *testing.T) {
	d := &digestServer{t: t}
	var target *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/redirect", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/target", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("/target", d.handler(nil))
	target = httptest.NewServer(mux)
	defer target.Close()

	body := strings.Repeat("redirect-me-", 1000)
	req, err := NewRequest("POST", target.URL+"/redirect", strings.NewReader(body))
	require.NoError(t, err)

	res, err := newReplayClient().DoRequest(req)
	require.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)
	require.Len(t, d.digests, 1)
	assert.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte(body))), d.digests[0])
}

// TestBodyReplayRedirect301ConvertsToGET asserts the existing 301/302/303
// method conversion behavior is preserved: body is dropped, method becomes GET.
func TestBodyReplayRedirect301ConvertsToGET(t *testing.T) {
	var gotMethod string
	var gotBodyLen int
	mux := http.NewServeMux()
	var target *httptest.Server
	mux.HandleFunc("/redirect", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/target", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/target", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		b, _ := io.ReadAll(r.Body)
		gotBodyLen = len(b)
		w.WriteHeader(http.StatusOK)
	})
	target = httptest.NewServer(mux)
	defer target.Close()

	req, err := NewRequest("POST", target.URL+"/redirect", strings.NewReader("payload"))
	require.NoError(t, err)

	res, err := newReplayClient().DoRequest(req)
	require.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)
	assert.Equal(t, "GET", gotMethod)
	assert.Equal(t, 0, gotBodyLen)
}

// TestBodyReplayExceedsLimit asserts bodies larger than L fail before any
// request is sent, without buffering the whole body in memory.
func TestBodyReplayExceedsLimit(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
	}))
	defer ts.Close()

	// Unknown length, non-seekable stream larger than L.
	req, err := NewRequest("POST", ts.URL, nil)
	require.NoError(t, err)
	req.Body = io.NopCloser(io.LimitReader(&infiniteReader{}, testReplayMaxSize+1))
	req.ContentLength = -1 // unknown length

	res, err := newReplayClient().DoRequest(req)
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds replay limit")
	assert.Equal(t, int32(0), atomic.LoadInt32(&count), "no request must be sent")
}

// TestBodyReplayDeclaredLengthMismatch asserts a declared Content-Length that
// does not match the actual body fails before any request is sent.
func TestBodyReplayDeclaredLengthMismatch(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
	}))
	defer ts.Close()

	req, err := NewRequest("POST", ts.URL, strings.NewReader("short"))
	require.NoError(t, err)
	req.ContentLength = 100 // lie about the length

	res, err := newReplayClient().DoRequest(req)
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match")
	assert.Equal(t, int32(0), atomic.LoadInt32(&count), "no request must be sent")
}

// TestBodyReplayReadError asserts a failing body reader fails the request
// before anything is sent, and the original body is still closed exactly once.
func TestBodyReplayReadError(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
	}))
	defer ts.Close()

	var closes int32
	src := &failReader{r: strings.NewReader(strings.Repeat("x", 4096)), limit: 100}
	req, err := NewRequest("POST", ts.URL, nil)
	require.NoError(t, err)
	req.Body = closeCounter{src, &closes}
	req.ContentLength = -1

	res, err := newReplayClient().DoRequest(req)
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading request body")
	assert.Equal(t, int32(0), atomic.LoadInt32(&count), "no request must be sent")
	assert.Equal(t, int32(1), atomic.LoadInt32(&closes), "original body closed exactly once")
}

// TestBodyReplayMemoryBound asserts the in-memory buffer never exceeds M
// (plus the fixed read chunk) and larger bodies spill to a temp file.
func TestBodyReplayMemoryBound(t *testing.T) {
	// Small body: stays in memory, capacity bounded by M, no temp file.
	small := newBodyReplayer(io.NopCloser(strings.NewReader("tiny")), testReplayMemLimit, testReplayMaxSize)
	require.NoError(t, small.prepare())
	assert.Nil(t, small.file)
	assert.LessOrEqual(t, int64(cap(small.mem)), int64(testReplayMemLimit))
	assert.Equal(t, int64(4), small.size)
	small.cleanup()

	// Large body: spills to temp file, memory released.
	big := newBodyReplayer(io.NopCloser(strings.NewReader(strings.Repeat("y", 100*1024))),
		testReplayMemLimit, testReplayMaxSize)
	require.NoError(t, big.prepare())
	require.NotNil(t, big.file, "body larger than M must spill to temp file")
	assert.Nil(t, big.mem, "memory buffer must be released after spill")
	assert.Equal(t, int64(100*1024), big.size)

	// Replays are complete and identical, even after a per-attempt Close.
	first, err := big.getBody()
	require.NoError(t, err)
	b1, err := io.ReadAll(first)
	require.NoError(t, err)
	require.NoError(t, first.Close(), "per-attempt Close must not fail")

	second, err := big.getBody()
	require.NoError(t, err, "per-attempt Close must not destroy the replay source")
	b2, err := io.ReadAll(second)
	require.NoError(t, err)
	require.NoError(t, second.Close())

	assert.Equal(t, 100*1024, len(b1))
	assert.Equal(t, b1, b2)

	// Terminal cleanup removes the temp file.
	name := big.file.Name()
	big.cleanup()
	_, err = os.Stat(name)
	assert.True(t, os.IsNotExist(err), "temp file must be removed on cleanup")
}

// TestBodyReplayTempFileRemovedAfterRequest asserts temp files and handles
// are released once the request (including retries) finishes.
func TestBodyReplayTempFileRemovedAfterRequest(t *testing.T) {
	d := &digestServer{t: t}
	ts := httptest.NewServer(d.handler([]int{500, 200}))
	defer ts.Close()

	req, err := NewRequest("POST", ts.URL, strings.NewReader(strings.Repeat("z", 50*1024)))
	require.NoError(t, err)

	c := newReplayClient()
	res, err := c.DoRequest(req)
	require.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)
	require.NotNil(t, req.bodyReplayer)
	assert.Nil(t, req.bodyReplayer.file, "temp file handle must be released")
	assert.Nil(t, req.bodyReplayer.mem, "memory buffer must be released")
	assert.True(t, req.bodyReplayer.cleaned)
}

// TestBodyReplayCancelledStopsRetry asserts cancelled requests are not retried.
func TestBodyReplayCancelledStopsRetry(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(500)
	}))
	defer ts.Close()

	req, err := NewRequest("POST", ts.URL, strings.NewReader("payload"))
	require.NoError(t, err)
	req.Cancel()

	_, _ = newReplayClient().DoRequest(req)
	assert.Equal(t, int32(1), atomic.LoadInt32(&count), "cancelled request must not be retried")
}

// TestBodyReplayContextCancelledStopsRetry asserts a cancelled context stops
// retries even when the server keeps returning a retryable status code.
func TestBodyReplayContextCancelledStopsRetry(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(503)
	}))
	defer ts.Close()

	req, err := NewRequest("POST", ts.URL, strings.NewReader("payload"))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(req.Context())
	req.Request = req.WithContext(ctx)

	c := newReplayClient()
	c.Transport = &cancelAfterFirst{RoundTripper: c.Transport, cancel: cancel}

	_, _ = c.DoRequest(req)
	assert.Equal(t, int32(1), atomic.LoadInt32(&count), "context cancellation must stop retries")
}

// cancelAfterFirst cancels ctx after the first round trip, before any retry.
type cancelAfterFirst struct {
	http.RoundTripper
	once   sync.Once
	cancel context.CancelFunc
}

func (c *cancelAfterFirst) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.RoundTripper.RoundTrip(r)
	c.once.Do(c.cancel)
	return resp, err
}

// nopSeekReader is a plain ReadCloser without GetBody or Seek support.
type nopSeekReader struct {
	io.Reader
}

func (nopSeekReader) Close() error { return nil }

// TestBodyReplayDisabledPreservesBehavior asserts that with replay disabled
// (the default), a body that cannot be replayed is lost on retry: the retry
// sends an empty body and the request fails. Replay mode fixes exactly this.
func TestBodyReplayDisabledPreservesBehavior(t *testing.T) {
	d := &digestServer{t: t}
	ts := httptest.NewServer(d.handler([]int{500, 200}))
	defer ts.Close()

	req, err := NewRequest("POST", ts.URL, nil)
	require.NoError(t, err)
	req.Body = nopSeekReader{strings.NewReader("payload")}
	req.ContentLength = 7

	res, err := newClientDefault().DoRequest(req)
	assert.Nil(t, res)
	require.Error(t, err)
	// First attempt carries the body; any further attempt reaches the server
	// with an empty body because the original stream is already consumed.
	require.GreaterOrEqual(t, d.count, 1)
	assert.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte("payload"))), d.digests[0])
	for _, dg := range d.digests[1:] {
		assert.Equal(t, fmt.Sprintf("%x", sha256.Sum256(nil)), dg, "retry body must be empty when replay is disabled")
	}
}

// TestBodyReplayInvalidConfigDisabled asserts invalid M/L disables replay.
func TestBodyReplayInvalidConfigDisabled(t *testing.T) {
	for _, opt := range []*Options{
		{BodyReplay: true, BodyReplayMemoryLimit: 0, BodyReplayMaxSize: 100},
		{BodyReplay: true, BodyReplayMemoryLimit: -1, BodyReplayMaxSize: 100},
		{BodyReplay: true, BodyReplayMemoryLimit: 200, BodyReplayMaxSize: 100},
	} {
		c := NewClient(opt)
		assert.False(t, c.opt.BodyReplay, "invalid config must disable replay")
	}
}

// infiniteReader is a non-seekable endless zero reader.
type infiniteReader struct{}

func (infiniteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
	}
	return len(p), nil
}

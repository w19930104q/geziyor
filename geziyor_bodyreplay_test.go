package geziyor_test

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/geziyor/geziyor"
	"github.com/geziyor/geziyor/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGeziyorBodyReplay verifies end-to-end that POST bodies are fully
// replayed on every retry through the Geziyor -> Client -> http stack.
func TestGeziyorBodyReplay(t *testing.T) {
	var (
		count   int32
		mu      sync.Mutex
		digests []string
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { // ignore robots.txt etc.
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		digests = append(digests, fmt.Sprintf("%x", sha256.Sum256(body)))
		mu.Unlock()
		switch atomic.AddInt32(&count, 1) {
		case 1:
			w.WriteHeader(http.StatusInternalServerError)
		case 2:
			w.WriteHeader(http.StatusBadGateway)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	body := strings.Repeat("geziyor-e2e-replay-", 1000) // ~18KB > M, forces temp file
	geziyor.NewGeziyor(&geziyor.Options{
		LogDisabled:           true,
		RobotsTxtDisabled:     true,
		BodyReplayEnabled:     true,
		BodyReplayMemoryLimit: 256,
		BodyReplayMaxSize:     1 << 20,
		StartRequestsFunc: func(g *geziyor.Geziyor) {
			g.Post(ts.URL, strings.NewReader(body), nil)
		},
		ParseFunc: func(g *geziyor.Geziyor, r *client.Response) {},
	}).Start()

	// 1 initial attempt + 2 retries, each carrying the identical full body.
	assert.Equal(t, int32(3), atomic.LoadInt32(&count))
	require.Len(t, digests, 3)
	expected := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
	for _, d := range digests {
		assert.Equal(t, expected, d)
	}
}

// TestGeziyorBodyReplayPrepError asserts preparation failures (here: body
// larger than L) are reported through the existing error channel without a
// request being sent and without being retried.
func TestGeziyorBodyReplayPrepError(t *testing.T) {
	var serverHits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&serverHits, 1)
	}))
	defer ts.Close()

	var (
		errMu sync.Mutex
		errs  []error
		errWg sync.WaitGroup
	)
	errWg.Add(1)
	g := geziyor.NewGeziyor(&geziyor.Options{
		LogDisabled:           true,
		RobotsTxtDisabled:     true,
		BodyReplayEnabled:     true,
		BodyReplayMemoryLimit: 64,
		BodyReplayMaxSize:     128, // body below will exceed it
		ErrorFunc: func(g *geziyor.Geziyor, r *client.Request, err error) {
			errMu.Lock()
			errs = append(errs, err)
			errMu.Unlock()
			errWg.Done()
		},
		StartRequestsFunc: func(g *geziyor.Geziyor) {
			g.Post(ts.URL, strings.NewReader(strings.Repeat("x", 1024)), nil)
		},
	})
	g.Start()
	errWg.Wait()

	errMu.Lock()
	require.Len(t, errs, 1, "preparation error must be reported exactly once")
	assert.Contains(t, errs[0].Error(), "exceeds replay limit")
	errMu.Unlock()
	assert.Equal(t, int32(0), atomic.LoadInt32(&serverHits), "no request must be sent")
}

package client

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// bodyReplayChunkSize is the fixed read chunk used while buffering request
// bodies for replay. Peak memory usage of a bodyReplayer is bounded by
// the configured memory threshold plus one chunk of this size.
const bodyReplayChunkSize = 32 * 1024

// bodyReplayer buffers a request body before the first network attempt so it
// can be replayed on retries and on 307/308 redirects. Up to memLimit bytes
// are held in memory; anything beyond that spills to a temporary file.
// Bodies larger than maxSize, read failures and declared length mismatches
// are reported as preparation errors and the request is never sent.
type bodyReplayer struct {
	src      io.ReadCloser // original caller-provided body
	mem      []byte        // in-memory buffer, never exceeds memLimit
	file     *os.File      // spillover file, created when memLimit is exceeded
	size     int64         // total buffered size
	memLimit int64         // M: memory threshold
	maxSize  int64         // L: total size limit
	srcDone  bool          // original body drained and closed
	cleaned  bool          // terminal cleanup performed
}

// newBodyReplayer wraps src so it can be replayed.
// memLimit (M) and maxSize (L) must satisfy 0 < M <= L.
func newBodyReplayer(src io.ReadCloser, memLimit, maxSize int64) *bodyReplayer {
	return &bodyReplayer{
		src:      src,
		memLimit: memLimit,
		maxSize:  maxSize,
	}
}

// prepare drains the original body into the memory buffer / temp file.
// The original body is closed exactly once, whether prepare succeeds or not.
func (r *bodyReplayer) prepare() (err error) {
	defer r.closeSrc()

	buf := make([]byte, bodyReplayChunkSize)
	for {
		n, readErr := r.src.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if r.size+int64(n) > r.maxSize {
				return fmt.Errorf("request body exceeds replay limit of %d bytes", r.maxSize)
			}
			if r.file == nil && int64(len(r.mem))+int64(n) > r.memLimit {
				if err = r.spill(); err != nil {
					return err
				}
			}
			if r.file != nil {
				if _, err = r.file.Write(chunk); err != nil {
					return fmt.Errorf("writing request body replay temp file: %w", err)
				}
			} else {
				r.mem = appendToBounded(r.mem, chunk, r.memLimit)
			}
			r.size += int64(n)
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("reading request body for replay: %w", readErr)
		}
	}
}

// appendToBounded appends chunk to buf without letting its capacity exceed
// limit, so the in-memory buffer never grows past M plus the fixed read chunk.
func appendToBounded(buf, chunk []byte, limit int64) []byte {
	need := len(buf) + len(chunk)
	if need > cap(buf) {
		newCap := 2 * cap(buf)
		if newCap < need {
			newCap = need
		}
		if int64(newCap) > limit {
			newCap = int(limit)
		}
		grown := make([]byte, len(buf), newCap)
		copy(grown, buf)
		buf = grown
	}
	return append(buf, chunk...)
}

// spill moves the in-memory buffer to a temporary file and releases the memory.
func (r *bodyReplayer) spill() error {
	f, err := os.CreateTemp("", "geziyor-body-replay-*")
	if err != nil {
		return fmt.Errorf("creating request body replay temp file: %w", err)
	}
	r.file = f
	if _, err := f.Write(r.mem); err != nil {
		return fmt.Errorf("writing request body replay temp file: %w", err)
	}
	r.mem = nil
	return nil
}

// getBody returns a fresh reader over the buffered body. It is assigned to
// http.Request.GetBody so the net/http stack replays the body on 307/308
// redirects, and is used by the client to rewind before each retry.
// Closing the returned reader does not release the underlying buffer or file.
func (r *bodyReplayer) getBody() (io.ReadCloser, error) {
	if r.file != nil {
		if _, err := r.file.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewinding request body replay temp file: %w", err)
		}
		return replayReadCloser{Reader: io.LimitReader(r.file, r.size)}, nil
	}
	return io.NopCloser(bytes.NewReader(r.mem)), nil
}

// replayReadCloser is a per-attempt body reader. Its Close is a no-op: the
// shared buffer/temp file is owned by bodyReplayer and only released by
// cleanup, so a single attempt's Close cannot destroy the replay source.
type replayReadCloser struct {
	io.Reader
}

func (replayReadCloser) Close() error { return nil }

// closeSrc closes the original caller-provided body exactly once.
func (r *bodyReplayer) closeSrc() {
	if r.srcDone {
		return
	}
	r.srcDone = true
	if r.src != nil {
		r.src.Close()
	}
}

// cleanup performs terminal resource cleanup: the original body is closed
// (if it somehow was not already) and the temp file is closed and removed.
func (r *bodyReplayer) cleanup() {
	if r.cleaned {
		return
	}
	r.cleaned = true
	r.closeSrc()
	if r.file != nil {
		name := r.file.Name()
		r.file.Close()
		os.Remove(name)
		r.file = nil
	}
	r.mem = nil
}

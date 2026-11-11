package geziyor_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/geziyor/geziyor"
	"github.com/geziyor/geziyor/client"
	"github.com/geziyor/geziyor/export"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lifecycleExporter is a controllable Exporter used by the lifecycle
// regression tests. It records every item it receives and can fail, panic,
// return immediately, or slow down on demand.
type lifecycleExporter struct {
	mu sync.Mutex

	got []interface{}

	// early makes Export return nil before reading anything.
	early bool
	// startErr makes Export return an error before reading anything.
	startErr error
	// failAt returns failErr after receiving failAt items (1-based).
	failAt  int
	failErr error
	// panicAt panics with panicVal after receiving panicAt items (1-based).
	panicAt  int
	panicVal interface{}
	// delay is applied after each received item to emulate a slow sink.
	delay time.Duration
}

func (e *lifecycleExporter) Export(items chan interface{}) error {
	if e.early {
		return nil
	}
	if e.startErr != nil {
		return e.startErr
	}

	received := 0
	for item := range items {
		e.mu.Lock()
		e.got = append(e.got, item)
		e.mu.Unlock()

		received++
		if e.delay > 0 {
			time.Sleep(e.delay)
		}
		if e.panicAt > 0 && received >= e.panicAt {
			panic(e.panicVal)
		}
		if e.failAt > 0 && received >= e.failAt {
			return e.failErr
		}
	}
	return nil
}

func (e *lifecycleExporter) received() []interface{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	items := make([]interface{}, len(e.got))
	copy(items, e.got)
	return items
}

func exporterResult(t *testing.T, g *geziyor.Geziyor, id string) geziyor.ExporterResult {
	t.Helper()
	for _, result := range g.ExportResults() {
		if result.ID == id {
			return result
		}
	}
	t.Fatalf("no exporter result with id %q", id)
	return geziyor.ExporterResult{}
}

func assertExporterOrder(t *testing.T, exporter *lifecycleExporter, expected []int) {
	t.Helper()
	got := exporter.received()
	require.Len(t, got, len(expected))
	for i, expectedItem := range expected {
		assert.Equal(t, expectedItem, got[i], "item at index %d", i)
	}
}

func newTestGeziyor(opts *geziyor.Options) *geziyor.Geziyor {
	opts.LogDisabled = true
	opts.RobotsTxtDisabled = true
	return geziyor.NewGeziyor(opts)
}

// TestExportersInterleavedFailuresDoNotStall covers several exporters
// succeeding, failing after different item counts, panicking, and returning
// early nil in the same run. Healthy exporters must still receive every item,
// in order, while broken exporters are dropped from dispatch.
func TestExportersInterleavedFailuresDoNotStall(t *testing.T) {
	defer leaktest.Check(t)()

	const itemCount = 50

	failErr := errors.New("exporter failed after two items")
	panicValue := errors.New("exporter panicked")
	healthyA := &lifecycleExporter{}
	failing := &lifecycleExporter{failAt: 2, failErr: failErr}
	panicking := &lifecycleExporter{panicAt: 3, panicVal: panicValue}
	early := &lifecycleExporter{early: true}
	healthyB := &lifecycleExporter{}

	g := newTestGeziyor(&geziyor.Options{
		Exporters:     []export.Exporter{healthyA, failing, panicking, early, healthyB},
		ExporterNames: []string{"healthy-a", "failing", "panicking", "early", "healthy-b"},
	})
	g.Opt.StartRequestsFunc = func(g *geziyor.Geziyor) {
		for i := 0; i < itemCount; i++ {
			g.Exports <- i
		}
	}
	g.Start()

	healthyOrder := make([]int, itemCount)
	for i := range healthyOrder {
		healthyOrder[i] = i
	}
	assertExporterOrder(t, healthyA, healthyOrder)
	assertExporterOrder(t, healthyB, healthyOrder)
	assert.Equal(t, []interface{}{0, 1}, failing.received())
	assert.Equal(t, []interface{}{0, 1, 2}, panicking.received())
	assert.Empty(t, early.received())

	results := g.ExportResults()
	require.Len(t, results, 5)

	healthyResult := exporterResult(t, g, "healthy-a")
	assert.True(t, healthyResult.Finished)
	assert.NoError(t, healthyResult.Err)
	assert.False(t, healthyResult.EarlyExit)

	failingResult := exporterResult(t, g, "failing")
	assert.True(t, failingResult.Finished)
	assert.True(t, failingResult.EarlyExit)
	assert.ErrorIs(t, failingResult.Err, failErr)

	panicResult := exporterResult(t, g, "panicking")
	assert.True(t, panicResult.Finished)
	assert.True(t, panicResult.EarlyExit)
	assert.Equal(t, panicValue, panicResult.Panic)
	require.Error(t, panicResult.Err)
	assert.ErrorIs(t, panicResult.Err, panicValue)
	assert.Contains(t, panicResult.Err.Error(), "panic")

	earlyResult := exporterResult(t, g, "early")
	assert.True(t, earlyResult.Finished)
	assert.NoError(t, earlyResult.Err)
	assert.True(t, earlyResult.EarlyExit, "an early successful exit must be diagnosable")

	errs := g.ExportErrors()
	require.Len(t, errs, 2)
	assert.ErrorIs(t, errs["failing"], failErr)
	assert.Error(t, errs["panicking"])
	assert.NotContains(t, errs, "healthy-a")
	assert.NotContains(t, errs, "healthy-b")
	assert.NotContains(t, errs, "early")

	aggregate := g.ExportError()
	require.Error(t, aggregate)
	assert.True(t, errors.Is(aggregate, failErr))
	var exporterErrs geziyor.ExporterErrors
	assert.True(t, errors.As(aggregate, &exporterErrs))
	assert.Len(t, exporterErrs, 2)
}

// TestExportersConcurrentProduction covers concurrent item production. Every
// healthy exporter must see every item exactly once, in the order the items
// entered Geziyor.Exports.
func TestExportersConcurrentProduction(t *testing.T) {
	defer leaktest.Check(t)()

	const itemCount = 40

	healthyA := &lifecycleExporter{}
	healthyB := &lifecycleExporter{}

	var (
		sendMu sync.Mutex
		sent   []int
		next   int
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer server.Close()

	g := newTestGeziyor(&geziyor.Options{
		ConcurrentRequests: 8,
		URLRevisitEnabled:  true,
		Exporters:          []export.Exporter{healthyA, healthyB},
		ExporterNames:      []string{"healthy-a", "healthy-b"},
	})
	g.Opt.ParseFunc = func(g *geziyor.Geziyor, r *client.Response) {
		sendMu.Lock()
		item := next
		next++
		sent = append(sent, item)
		g.Exports <- item
		sendMu.Unlock()
	}
	g.Opt.StartRequestsFunc = func(g *geziyor.Geziyor) {
		for i := 0; i < itemCount; i++ {
			g.Get(fmt.Sprintf("%s/?item=%d", server.URL, i), g.Opt.ParseFunc)
		}
	}
	g.Start()

	require.Len(t, sent, itemCount)
	assertExporterOrder(t, healthyA, sent)
	assertExporterOrder(t, healthyB, sent)

	for _, id := range []string{"healthy-a", "healthy-b"} {
		result := exporterResult(t, g, id)
		assert.True(t, result.Finished)
		assert.NoError(t, result.Err)
		assert.False(t, result.EarlyExit)
	}
	assert.NoError(t, g.ExportError())
}

// TestSlowExporterAppliesBackpressure verifies dispatch does not buffer around
// a slow exporter (no unbounded queue, no goroutine per item). Start must wait
// for the slow exporter, so the run cannot finish before it drains.
func TestSlowExporterAppliesBackpressure(t *testing.T) {
	defer leaktest.Check(t)()

	const (
		itemCount = 8
		delay     = 10 * time.Millisecond
	)

	slow := &lifecycleExporter{delay: delay}
	fast := &lifecycleExporter{}

	g := newTestGeziyor(&geziyor.Options{
		Exporters:     []export.Exporter{slow, fast},
		ExporterNames: []string{"slow", "fast"},
	})
	g.Opt.StartRequestsFunc = func(g *geziyor.Geziyor) {
		for i := 0; i < itemCount; i++ {
			g.Exports <- i
		}
	}

	startedAt := time.Now()
	g.Start()
	elapsed := time.Since(startedAt)

	assert.GreaterOrEqual(t, elapsed, itemCount*delay,
		"dispatch finished before the slow exporter drained its items")

	expected := make([]int, itemCount)
	for i := range expected {
		expected[i] = i
	}
	assertExporterOrder(t, slow, expected)
	assertExporterOrder(t, fast, expected)

	for _, id := range []string{"slow", "fast"} {
		result := exporterResult(t, g, id)
		assert.True(t, result.Finished)
		assert.NoError(t, result.Err)
		assert.False(t, result.EarlyExit)
	}
}

// TestAllExportersFailStillFinishesCollection ensures a fully failed exporter
// set cannot deadlock the scraper; items are drained and Start returns.
func TestAllExportersFailStillFinishesCollection(t *testing.T) {
	defer leaktest.Check(t)()

	const itemCount = 30

	firstErr := errors.New("first exporter failed")
	secondErr := errors.New("second exporter failed")
	first := &lifecycleExporter{startErr: firstErr}
	second := &lifecycleExporter{startErr: secondErr}

	g := newTestGeziyor(&geziyor.Options{
		Exporters:     []export.Exporter{first, second},
		ExporterNames: []string{"first", "second"},
	})
	g.Opt.StartRequestsFunc = func(g *geziyor.Geziyor) {
		for i := 0; i < itemCount; i++ {
			g.Exports <- i
		}
	}

	done := make(chan struct{})
	go func() {
		g.Start()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("collection did not finish after all exporters failed")
	}

	assert.Empty(t, first.received())
	assert.Empty(t, second.received())

	errs := g.ExportErrors()
	require.Len(t, errs, 2)
	assert.ErrorIs(t, errs["first"], firstErr)
	assert.ErrorIs(t, errs["second"], secondErr)

	aggregate := g.ExportError()
	require.Error(t, aggregate)
	assert.True(t, errors.Is(aggregate, firstErr))
	assert.True(t, errors.Is(aggregate, secondErr))
}

// TestNoExportersStillDrainsExports keeps the no-exporter path working after
// the dispatch refactor: producers must not block and Start must finish.
func TestNoExportersStillDrainsExports(t *testing.T) {
	defer leaktest.Check(t)()

	const itemCount = 20
	g := newTestGeziyor(&geziyor.Options{})
	g.Opt.StartRequestsFunc = func(g *geziyor.Geziyor) {
		for i := 0; i < itemCount; i++ {
			g.Exports <- i
		}
	}
	g.Start()

	assert.Nil(t, g.ExportResults())
	assert.NoError(t, g.ExportError())
}

// TestBuiltInExporterWriteAndFlushErrorsVisible drives the built-in file
// exporters into write failures with /dev/full. CSV's actual file write
// happens during Flush, so this also covers flush-error visibility.
func TestBuiltInExporterWriteAndFlushErrorsVisible(t *testing.T) {
	defer leaktest.Check(t)()

	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("/dev/full is not available on this platform")
	}

	csvExporter := &export.CSV{FileName: "/dev/full"}
	jsonExporter := &export.JSON{FileName: "/dev/full"}
	jsonLineExporter := &export.JSONLine{FileName: "/dev/full"}

	g := newTestGeziyor(&geziyor.Options{
		Exporters:     []export.Exporter{csvExporter, jsonExporter, jsonLineExporter},
		ExporterNames: []string{"csv", "json", "jsonline"},
	})
	g.Opt.StartRequestsFunc = func(g *geziyor.Geziyor) {
		g.Exports <- map[string]string{"key": "value"}
	}
	g.Start()

	results := g.ExportResults()
	require.Len(t, results, 3)
	for _, result := range results {
		assert.True(t, result.Finished, "exporter %s did not finish", result.ID)
		require.Error(t, result.Err, "exporter %s hid its write error", result.ID)
		assert.True(t, errors.Is(result.Err, syscall.ENOSPC),
			"exporter %s error should retain the underlying ENOSPC: %v", result.ID, result.Err)
	}

	csvResult := exporterResult(t, g, "csv")
	assert.Contains(t, csvResult.Err.Error(), "flush", "CSV flush failure must be reported")

	errs := g.ExportErrors()
	require.Len(t, errs, 3)
	for _, id := range []string{"csv", "json", "jsonline"} {
		assert.Error(t, errs[id])
	}
	assert.Error(t, g.ExportError())
}

package geziyor

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/geziyor/geziyor/export"
	"github.com/geziyor/geziyor/internal"
)

// ExporterResult reports the final state of a single configured Exporter.
//
// Err keeps the exact error returned by Exporter.Export. Panic keeps the
// recovered panic value when the exporter panicked instead of returning.
// EarlyExit is true when Export returned before Geziyor finished delivering
// items (for example an exporter that returns nil immediately). Such an
// early success does not stop the rest of the scrape, but it is visible here
// so it can be diagnosed.
type ExporterResult struct {
	// Index is the position of the exporter in Options.Exporters.
	Index int
	// ID identifies the exporter. It is Options.ExporterNames[Index] when set,
	// otherwise a generated "<type>#<index>" identifier.
	ID string
	// Exporter is the configured exporter instance.
	Exporter export.Exporter
	// Err is the original error returned by Exporter.Export, or the error
	// describing a recovered panic. It is nil for a successful exporter.
	Err error
	// Panic is the recovered panic value when Exporter.Export panicked.
	Panic interface{}
	// EarlyExit is true when Export returned before the dispatch loop saw
	// Geziyor.Exports closed.
	EarlyExit bool
	// Finished is true once Exporter.Export has returned (or panicked and was
	// recovered).
	Finished bool
}

// ExporterError attributes one original exporter error to the exporter that
// produced it.
type ExporterError struct {
	// ID is the exporter identifier (see ExporterResult.ID).
	ID string
	// Index is the position of the exporter in Options.Exporters.
	Index int
	// Exporter is the configured exporter instance that failed.
	Exporter export.Exporter
	// Err is the original error returned by Exporter.Export.
	Err error
}

func (e *ExporterError) Error() string {
	return fmt.Sprintf("exporter %q: %v", e.ID, e.Err)
}

// Unwrap returns the original exporter error.
func (e *ExporterError) Unwrap() error { return e.Err }

// ExporterErrors aggregates errors from several exporters while keeping each
// original error available through Unwrap and through the slice elements.
type ExporterErrors []*ExporterError

func (e ExporterErrors) Error() string {
	switch len(e) {
	case 0:
		return ""
	case 1:
		return e[0].Error()
	default:
		msg := fmt.Sprintf("%d exporter errors:", len(e))
		for _, err := range e {
			msg += " " + err.Error() + ";"
		}
		return msg
	}
}

// Unwrap exposes every original error so errors.Is/errors.As can traverse the
// aggregate.
func (e ExporterErrors) Unwrap() []error {
	errs := make([]error, 0, len(e))
	for _, err := range e {
		errs = append(errs, err)
	}
	return errs
}

// Is lets errors.Is match any original exporter error even with Go versions
// that only understand single-error unwrapping.
func (e ExporterErrors) Is(target error) bool {
	for _, err := range e {
		if errors.Is(err.Err, target) {
			return true
		}
	}
	return false
}

// exporterSink is the dispatch endpoint for one exporter.
type exporterSink struct {
	index    int
	id       string
	exporter export.Exporter
	// items is intentionally unbuffered so a slow exporter applies
	// backpressure to the whole dispatch loop instead of accumulating an
	// unbounded queue.
	items chan interface{}
	// done is closed when Export returns (normally, with an error, or after a
	// recovered panic). The dispatcher uses it to stop sending to that
	// exporter without ever sending on a closed channel.
	done chan struct{}
}

func (g *Geziyor) exporterIdentifier(index int, exporter export.Exporter) string {
	if index < len(g.Opt.ExporterNames) && g.Opt.ExporterNames[index] != "" {
		return g.Opt.ExporterNames[index]
	}
	return fmt.Sprintf("%T#%d", exporter, index)
}

func (g *Geziyor) startExporters() {
	if len(g.Opt.Exporters) == 0 {
		// With no exporter, still drain the channel so producers never block
		// and the collection can finish.
		g.wgDispatch.Add(1)
		go func() {
			defer g.wgDispatch.Done()
			for range g.Exports {
			}
		}()
		return
	}

	sinks := make([]*exporterSink, len(g.Opt.Exporters))
	results := make([]ExporterResult, len(g.Opt.Exporters))
	for i, exporter := range g.Opt.Exporters {
		id := g.exporterIdentifier(i, exporter)
		sinks[i] = &exporterSink{
			index:    i,
			id:       id,
			exporter: exporter,
			items:    make(chan interface{}),
			done:     make(chan struct{}),
		}
		results[i] = ExporterResult{Index: i, ID: id, Exporter: exporter}
	}
	g.exportMu.Lock()
	g.exportResults = results
	g.exportMu.Unlock()

	g.wgExporters.Add(len(sinks))
	for _, sink := range sinks {
		go g.runExporter(sink)
	}

	g.wgDispatch.Add(1)
	go g.dispatchExports(sinks)
}

// runExporter calls Export and records its final outcome. A panic is recovered
// so one broken exporter cannot crash the scrape; closing sink.done right after
// guarantees the dispatcher stops delivering to it even if it exited early.
func (g *Geziyor) runExporter(sink *exporterSink) {
	defer g.wgExporters.Done()
	defer close(sink.done)

	var (
		err      error
		panicVal interface{}
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicVal = r
				if panicErr, ok := r.(error); ok {
					err = fmt.Errorf("exporter panic: %w", panicErr)
				} else {
					err = fmt.Errorf("exporter panic: %v", r)
				}
			}
		}()
		err = sink.exporter.Export(sink.items)
	}()

	// Export may legitimately return before the input is closed (for example
	// a no-op exporter). Mark that case so the successful early exit is
	// visible through ExportResults.
	earlyExit := atomic.LoadUint32(&g.exportInputClosed) == 0

	g.exportMu.Lock()
	g.exportResults[sink.index] = ExporterResult{
		Index:     sink.index,
		ID:        sink.id,
		Exporter:  sink.exporter,
		Err:       err,
		Panic:     panicVal,
		EarlyExit: earlyExit,
		Finished:  true,
	}
	g.exportMu.Unlock()

	if err != nil {
		internal.Logger.Printf("exporter %s error: %v\n", sink.id, err)
	}
}

// dispatchExports fans out items from Geziyor.Exports to all exporter sinks.
//
// The loop owns both sending and closing of sink.items. It never sends to an
// exporter whose done channel is closed, so a failed, panicked, or
// early-returning exporter cannot block or poison the dispatch loop.
// Sends are unbuffered: a slow exporter blocks the loop (backpressure) rather
// than making memory grow or spawning one goroutine per item.
func (g *Geziyor) dispatchExports(sinks []*exporterSink) {
	defer g.wgDispatch.Done()

	stopped := make([]bool, len(sinks))
	for item := range g.Exports {
		for i, sink := range sinks {
			if stopped[i] {
				continue
			}
			// Prefer an already-finished exporter over a send, so a
			// pathological exporter implementation that leaves a reader
			// behind cannot keep receiving after Export has returned.
			select {
			case <-sink.done:
				stopped[i] = true
				continue
			default:
			}
			select {
			case sink.items <- item:
			case <-sink.done:
				stopped[i] = true
			}
		}
	}

	// Input is fully drained. From here on Export returning before the sink
	// channel is closed counts as an early exit, and no exporter can be sent
	// another item.
	atomic.StoreUint32(&g.exportInputClosed, 1)
	for _, sink := range sinks {
		close(sink.items)
	}
}

// ExportResults returns a snapshot of every configured exporter's final state.
// It is safe to call while a scrape is running; unfinished exporters have
// Finished == false.
func (g *Geziyor) ExportResults() []ExporterResult {
	g.exportMu.Lock()
	defer g.exportMu.Unlock()

	if len(g.exportResults) == 0 {
		return nil
	}
	results := make([]ExporterResult, len(g.exportResults))
	copy(results, g.exportResults)
	return results
}

// ExportErrors returns the original error from every exporter that failed,
// keyed by the exporter identifier (see ExporterResult.ID).
func (g *Geziyor) ExportErrors() map[string]error {
	results := g.ExportResults()
	errs := make(map[string]error)
	for _, result := range results {
		if result.Err != nil {
			errs[result.ID] = result.Err
		}
	}
	return errs
}

// ExportError returns nil when every exporter succeeded. Otherwise it returns
// an ExporterErrors aggregate that preserves each exporter's identifier and
// original error. It can be called after Start; it is also safe to call while
// the scrape is running.
func (g *Geziyor) ExportError() error {
	results := g.ExportResults()
	var errs ExporterErrors
	for _, result := range results {
		if result.Err != nil {
			errs = append(errs, &ExporterError{
				ID:       result.ID,
				Index:    result.Index,
				Exporter: result.Exporter,
				Err:      result.Err,
			})
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

// Err is an alias for ExportError, convenient when Start is used as a
// statement and the caller wants the final exporter error afterwards.
func (g *Geziyor) Err() error { return g.ExportError() }

// ExporterResults is an alias for ExportResults.
func (g *Geziyor) ExporterResults() []ExporterResult { return g.ExportResults() }

// ExporterErrors is an alias for ExportErrors.
func (g *Geziyor) ExporterErrors() map[string]error { return g.ExportErrors() }

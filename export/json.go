package export

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/geziyor/geziyor/internal"
)

// JSONLine exports response data as JSON streaming file
type JSONLine struct {
	FileName   string
	EscapeHTML bool
	Prefix     string
	Indent     string
}

// Export exports response data as JSON streaming file
func (e *JSONLine) Export(exports chan interface{}) (err error) {

	// Create or append file
	file, err := os.OpenFile(internal.DefaultString(e.FileName, "out.json"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("output file creation error: %w", err)
	}
	defer func() { err = closeExportFile(file, err) }()

	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(e.EscapeHTML)
	encoder.SetIndent(e.Prefix, e.Indent)

	// Export data as responses came
	for res := range exports {
		if encodeErr := encoder.Encode(res); encodeErr != nil {
			err = fmt.Errorf("JSON encoding error on exporter: %w", encodeErr)
			return err
		}
	}

	return nil
}

// JSON exports response data as JSON
type JSON struct {
	FileName   string
	EscapeHTML bool
}

// Export exports response data as JSON
func (e *JSON) Export(exports chan interface{}) (err error) {

	// Create or append file
	file, err := os.OpenFile(internal.DefaultString(e.FileName, "out.json"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("output file creation error: %w", err)
	}
	defer func() { err = closeExportFile(file, err) }()

	if _, err = file.Write([]byte("[\n")); err != nil {
		err = fmt.Errorf("file write error: %w", err)
		return err
	}

	// Keep at most one item pending so commas separate items instead of
	// trailing the final item. This keeps the output valid JSON without
	// buffering the whole export.
	var pending interface{}
	hasPending := false
	writePending := func(comma bool) error {
		data, marshalErr := jsonMarshalLine(pending, e.EscapeHTML, comma)
		if marshalErr != nil {
			return fmt.Errorf("JSON encoding error on exporter: %w", marshalErr)
		}
		if _, writeErr := file.Write(data); writeErr != nil {
			return fmt.Errorf("file write error: %w", writeErr)
		}
		return nil
	}

	// Export data as responses came
	for res := range exports {
		if hasPending {
			if err = writePending(true); err != nil {
				return err
			}
		}
		pending = res
		hasPending = true
	}
	if hasPending {
		if err = writePending(false); err != nil {
			return err
		}
	}

	if _, err = file.Write([]byte("]\n")); err != nil {
		err = fmt.Errorf("file write error: %w", err)
		return err
	}

	return nil
}

// jsonMarshalLine adds tab and optional comma/newline around actual data
func jsonMarshalLine(t interface{}, escapeHTML bool, comma bool) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(escapeHTML)

	buffer.Write([]byte("	")) // Tab char
	err := encoder.Encode(t)  // Write actual data
	if err != nil {
		return nil, err
	}
	buffer.Truncate(buffer.Len() - 1) // Remove last newline char
	if comma {
		buffer.Write([]byte(",\n")) // Write comma and newline char
	} else {
		buffer.Write([]byte("\n"))
	}

	return buffer.Bytes(), nil
}

// closeExportFile merges a file close error with the exporter's existing error.
// This makes write/flush/close failures all visible instead of silently
// discarding the deferred Close result.
func closeExportFile(file io.Closer, err error) error {
	closeErr := file.Close()
	if closeErr == nil {
		return err
	}
	closeErr = fmt.Errorf("output file close error: %w", closeErr)
	if err == nil {
		return closeErr
	}
	return fmt.Errorf("%v; %w", err, closeErr)
}

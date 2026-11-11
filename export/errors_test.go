package export

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeErrorWriter struct {
	err error
}

func (c closeErrorWriter) Close() error { return c.err }

func TestCloseExportFileKeepsCloseError(t *testing.T) {
	closeErr := errors.New("close failed")

	err := closeExportFile(closeErrorWriter{err: closeErr}, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, closeErr)
	assert.Contains(t, err.Error(), "output file close error")

	writeErr := errors.New("write failed")
	err = closeExportFile(closeErrorWriter{err: closeErr}, writeErr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), writeErr.Error())
	assert.ErrorIs(t, err, closeErr)
}

func TestJSONExporterMultipleItemsProducesValidJSON(t *testing.T) {
	fileName := filepath.Join(t.TempDir(), "out.json")
	exporter := &JSON{FileName: fileName}

	exports := make(chan interface{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- exporter.Export(exports)
	}()

	exports <- map[string]string{"a": "1"}
	exports <- map[string]string{"b": "2"}
	close(exports)

	require.NoError(t, <-errCh)

	contents, err := ioutil.ReadFile(fileName)
	require.NoError(t, err)

	var decoded []map[string]string
	require.NoError(t, json.Unmarshal(contents, &decoded), "output is not valid JSON: %s", contents)
	require.Len(t, decoded, 2)
	assert.Equal(t, map[string]string{"a": "1"}, decoded[0])
	assert.Equal(t, map[string]string{"b": "2"}, decoded[1])
}

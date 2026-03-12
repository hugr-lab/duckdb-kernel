package kernel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/hugr-lab/duckdb-kernel/internal/spool"
)

const maxArrowRows = 5_000_000

// ArrowServer serves Arrow IPC files over HTTP directly from the spool.
// Also serves static assets (perspective JS/WASM) for VS Code renderer.
type ArrowServer struct {
	spool    *spool.Spool
	listener net.Listener
	server   *http.Server
}

// NewArrowServer creates and starts an HTTP server on a random port.
func NewArrowServer(sp *spool.Spool) (*ArrowServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	as := &ArrowServer{
		spool:    sp,
		listener: ln,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/arrow", as.handleArrow)
	mux.HandleFunc("/arrow/stream", as.handleArrowStream)

	// Serve perspective static files if available.
	// Look next to the binary: <binary_dir>/static/perspective/
	if exePath, err := os.Executable(); err == nil {
		staticDir := filepath.Join(filepath.Dir(exePath), "static", "perspective")
		if info, err := os.Stat(staticDir); err == nil && info.IsDir() {
			fs := http.StripPrefix("/static/perspective/", http.FileServer(http.Dir(staticDir)))
			mux.Handle("/static/perspective/", addCORS(fs))
			log.Printf("Serving perspective static files from %s", staticDir)
		}
	}

	as.server = &http.Server{Handler: mux}

	go func() {
		if err := as.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("arrow http server error: %v", err)
		}
	}()

	log.Printf("Arrow HTTP server listening on %s", ln.Addr().String())
	return as, nil
}

// Port returns the port the server is listening on.
func (as *ArrowServer) Port() int {
	return as.listener.Addr().(*net.TCPAddr).Port
}

// Close shuts down the HTTP server.
func (as *ArrowServer) Close() error {
	return as.server.Close()
}

// handleArrow serves an Arrow IPC file as a single response (for small datasets).
// GET /arrow?q=<queryID>
func (as *ArrowServer) handleArrow(w http.ResponseWriter, r *http.Request) {
	queryID := r.URL.Query().Get("q")
	if queryID == "" {
		http.Error(w, "missing q parameter", http.StatusBadRequest)
		return
	}

	path := as.spool.Path(queryID)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	as.streamRawFile(w, path)
}

// handleArrowStream serves Arrow data as length-prefixed chunks for streaming.
// Each chunk is a complete Arrow IPC stream (schema + one batch + EOS).
// Browser reads via ReadableStream and calls table.update() per chunk.
//
// Wire format: [4-byte little-endian length][Arrow IPC bytes] repeated.
// A final 4-byte zero length signals end of stream.
//
// GET /arrow/stream?q=<queryID>&limit=<maxRows>
func (as *ArrowServer) handleArrowStream(w http.ResponseWriter, r *http.Request) {
	queryID := r.URL.Query().Get("q")
	if queryID == "" {
		http.Error(w, "missing q parameter", http.StatusBadRequest)
		return
	}

	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}

	reader, closer, err := as.spool.OpenReader(queryID)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer closer.Close()
	defer reader.Release()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Transfer-Encoding", "chunked")

	flusher, canFlush := w.(http.Flusher)
	schema := reader.Schema()

	written := 0
	for reader.Next() {
		rec := reader.RecordBatch()
		rows := int(rec.NumRows())

		writeRec := rec
		last := false
		sliced := false

		if limit > 0 && written+rows > limit {
			need := limit - written
			if need <= 0 {
				break
			}
			writeRec = rec.NewSlice(0, int64(need))
			sliced = true
			rows = need
			last = true
		}

		// Serialize this batch as a complete Arrow IPC stream.
		var buf bytes.Buffer
		w2 := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		if err := w2.Write(writeRec); err != nil {
			if sliced {
				writeRec.Release()
			}
			log.Printf("arrow stream: write batch error: %v", err)
			break
		}
		w2.Close()

		if sliced {
			writeRec.Release()
		}

		// Write length prefix + data.
		chunk := buf.Bytes()
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(chunk)))
		w.Write(lenBuf)
		w.Write(chunk)

		if canFlush {
			flusher.Flush()
		}

		written += rows
		if last {
			break
		}
	}

	// Write zero-length terminator.
	w.Write([]byte{0, 0, 0, 0})
	if canFlush {
		flusher.Flush()
	}
}

// streamRawFile sends the Arrow IPC file directly without parsing.
func (as *ArrowServer) streamRawFile(w http.ResponseWriter, path string) {
	f, err := os.Open(path)
	if err != nil {
		log.Printf("arrow: open error: %v", err)
		return
	}
	defer f.Close()

	if info, err := f.Stat(); err == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	}

	if _, err := io.Copy(w, f); err != nil {
		log.Printf("arrow: copy error: %v", err)
	}
}

// BaseURL returns the base URL of the Arrow HTTP server.
func (as *ArrowServer) BaseURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", as.Port())
}

// addCORS wraps an http.Handler to add CORS headers.
func addCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		h.ServeHTTP(w, r)
	})
}

// ArrowURL returns the URL for fetching the given query's Arrow data.
// Uses streaming endpoint for large datasets, raw file for small ones.
func (as *ArrowServer) ArrowURL(queryID string, totalRows int64) string {
	if totalRows > maxArrowRows {
		return fmt.Sprintf(
			"http://127.0.0.1:%d/arrow/stream?q=%s&limit=%d&total=%d",
			as.Port(), queryID, maxArrowRows, totalRows,
		)
	}
	return fmt.Sprintf("http://127.0.0.1:%d/arrow/stream?q=%s", as.Port(), queryID)
}

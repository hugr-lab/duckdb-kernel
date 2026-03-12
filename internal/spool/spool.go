package spool

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

const (
	DefaultMaxFiles = 5
	DefaultMaxAge   = 1 * time.Hour
)

// Spool manages temporary Arrow IPC files for a session.
type Spool struct {
	Dir      string
	MaxFiles int
	MaxAge   time.Duration
}

// NewSpool creates a new result spool for the given session.
func NewSpool(sessionID string) (*Spool, error) {
	sessionID = filepath.Base(sessionID)
	if sessionID == "." || sessionID == ".." || strings.ContainsAny(sessionID, `/\`) {
		return nil, fmt.Errorf("invalid session ID: %q", sessionID)
	}
	dir := filepath.Join(os.TempDir(), "duckdb-kernel", sessionID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create spool dir: %w", err)
	}
	return &Spool{
		Dir:      dir,
		MaxFiles: DefaultMaxFiles,
		MaxAge:   DefaultMaxAge,
	}, nil
}

// StreamWriter writes Arrow record batches to an IPC streaming file.
// Batches are flushed to disk immediately — no accumulation in memory.
type StreamWriter struct {
	w  *ipc.Writer
	f  *os.File
}

// NewStreamWriter creates a streaming IPC writer for the given query.
// Schema is taken from the first Write call.
func (s *Spool) NewStreamWriter(queryID string) (*StreamWriter, error) {
	path := s.Path(queryID)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create spool file: %w", err)
	}
	return &StreamWriter{f: f}, nil
}

// Write writes a single record batch to the IPC stream.
func (sw *StreamWriter) Write(rec arrow.Record) error {
	if sw.w == nil {
		sw.w = ipc.NewWriter(sw.f, ipc.WithSchema(rec.Schema()))
	}
	return sw.w.Write(rec)
}

// Close flushes and closes the IPC stream and underlying file.
func (sw *StreamWriter) Close() error {
	var errs []error
	if sw.w != nil {
		if err := sw.w.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if sw.f != nil {
		if err := sw.f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// OpenReader opens a streaming IPC reader for the given query.
func (s *Spool) OpenReader(queryID string) (*ipc.Reader, io.Closer, error) {
	path := s.Path(queryID)
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	r, err := ipc.NewReader(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return r, f, nil
}

// Path returns the Arrow IPC file path for a given query ID.
func (s *Spool) Path(queryID string) string {
	return filepath.Join(s.Dir, queryID+".arrow")
}

// Cleanup removes files exceeding the max count and max age limits.
func (s *Spool) Cleanup() error {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return fmt.Errorf("read spool dir: %w", err)
	}

	type fileInfo struct {
		path    string
		modTime time.Time
	}

	var files []fileInfo
	now := time.Now()

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(s.Dir, entry.Name())

		if now.Sub(info.ModTime()) > s.MaxAge {
			os.Remove(path)
			continue
		}

		files = append(files, fileInfo{path: path, modTime: info.ModTime()})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.After(files[j].modTime)
	})

	for i := s.MaxFiles; i < len(files); i++ {
		os.Remove(files[i].path)
	}

	return nil
}

// Destroy removes the entire spool directory.
func (s *Spool) Destroy() error {
	return os.RemoveAll(s.Dir)
}

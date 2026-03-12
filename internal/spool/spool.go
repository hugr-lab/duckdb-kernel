package spool

import (
	"bytes"
	"fmt"
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
	// Sanitize session ID to prevent path traversal
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

// Write writes Arrow records to an IPC file.
func (s *Spool) Write(queryID string, records []arrow.Record) error {
	if len(records) == 0 {
		return nil
	}

	path := s.Path(queryID)
	buf := &bytes.Buffer{}

	fw, err := ipc.NewFileWriter(buf, ipc.WithSchema(records[0].Schema()))
	if err != nil {
		return fmt.Errorf("create arrow writer: %w", err)
	}

	for _, rec := range records {
		if err := fw.Write(rec); err != nil {
			fw.Close()
			return fmt.Errorf("write record: %w", err)
		}
	}

	if err := fw.Close(); err != nil {
		return fmt.Errorf("close arrow writer: %w", err)
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	return nil
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

		// Remove files older than MaxAge
		if now.Sub(info.ModTime()) > s.MaxAge {
			os.Remove(path)
			continue
		}

		files = append(files, fileInfo{path: path, modTime: info.ModTime()})
	}

	// Sort by modification time (newest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.After(files[j].modTime)
	})

	// Remove excess files
	for i := s.MaxFiles; i < len(files); i++ {
		os.Remove(files[i].path)
	}

	return nil
}

// Destroy removes the entire spool directory.
func (s *Spool) Destroy() error {
	return os.RemoveAll(s.Dir)
}

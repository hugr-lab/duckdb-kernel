package spool

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Default configuration.
const (
	DefaultTTL     = 24 * time.Hour
	DefaultMaxSize = 2 * 1024 * 1024 * 1024 // 2 GB
	DefaultDir     = "" // empty = os.TempDir()/duckdb-kernel
)

// Config holds spool configuration.
type Config struct {
	Dir     string        // spool directory (default: /tmp/duckdb-kernel)
	TTL     time.Duration // max file age (default: 24h)
	MaxSize int64         // max total disk usage in bytes (default: 2GB)
}

// Spool manages Arrow IPC files on disk.
// Flat directory structure — no session nesting.
// Files are named {queryID}.arrow.
// TTL-based cleanup + disk size limit.
//
// Supports two directories:
//   - Dir (volatile): /tmp/duckdb-kernel/ — TTL cleanup, auto-managed
//   - PersistentDir (optional): .duckdb-results/ — user-pinned results, no TTL
type Spool struct {
	Dir           string
	PersistentDir string // optional, set via config or Pin()
	TTL           time.Duration
	MaxSize       int64
}

// NewSpool creates a spool with the given config.
// Creates the directory if it doesn't exist.
// Runs initial cleanup (delete expired files, enforce size limit).
func NewSpool(cfg Config) (*Spool, error) {
	dir := cfg.Dir
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "duckdb-kernel")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create spool dir: %w", err)
	}

	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	maxSize := cfg.MaxSize
	if maxSize <= 0 {
		maxSize = DefaultMaxSize
	}

	s := &Spool{Dir: dir, TTL: ttl, MaxSize: maxSize}

	// Initial cleanup on startup
	if n, err := s.Cleanup(); err != nil {
		log.Printf("spool cleanup warning: %v", err)
	} else if n > 0 {
		log.Printf("spool: cleaned up %d expired/excess files", n)
	}

	// Log surviving files
	if files, err := s.ListFiles(); err == nil && len(files) > 0 {
		log.Printf("spool: %d existing result files available", len(files))
	}

	return s, nil
}

// StreamWriter writes Arrow record batches to an IPC streaming file.
type StreamWriter struct {
	w *ipc.Writer
	f *os.File
}

// NewStreamWriter creates a streaming IPC writer for the given query.
func (s *Spool) NewStreamWriter(queryID string) (*StreamWriter, error) {
	path := s.Path(queryID)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create spool file: %w", err)
	}
	return &StreamWriter{f: f}, nil
}

// Write writes a single record batch to the IPC stream.
func (sw *StreamWriter) Write(rec arrow.RecordBatch) error {
	if sw.w == nil {
		sw.w = ipc.NewWriter(sw.f, ipc.WithSchema(rec.Schema()), ipc.WithLZ4())
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
// Checks persistent dir first (pinned results), then volatile dir.
func (s *Spool) OpenReader(queryID string) (*ipc.Reader, io.Closer, error) {
	// Try persistent dir first
	if s.PersistentDir != "" {
		path := filepath.Join(s.PersistentDir, filepath.Base(queryID)+".arrow")
		if f, err := os.Open(path); err == nil {
			r, err := ipc.NewReader(f)
			if err != nil {
				f.Close()
			} else {
				return r, f, nil
			}
		}
	}

	// Volatile dir
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
	return filepath.Join(s.Dir, filepath.Base(queryID)+".arrow")
}

// Exists checks if a spool file exists for the given query ID.
func (s *Spool) Exists(queryID string) bool {
	_, err := os.Stat(s.Path(queryID))
	return err == nil
}

// Remove deletes a single spool file by query ID (from both dirs).
func (s *Spool) Remove(queryID string) error {
	os.Remove(s.Path(queryID))
	if s.PersistentDir != "" {
		os.Remove(filepath.Join(s.PersistentDir, filepath.Base(queryID)+".arrow"))
	}
	return nil
}

// Pin copies an Arrow file from volatile dir to persistent dir.
// Creates persistent dir and .gitignore if needed.
func (s *Spool) Pin(queryID string) error {
	if s.PersistentDir == "" {
		return fmt.Errorf("persistent dir not configured")
	}
	src := s.Path(queryID)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("source file not found: %w", err)
	}

	// Create persistent dir
	if err := os.MkdirAll(s.PersistentDir, 0o755); err != nil {
		return fmt.Errorf("create persistent dir: %w", err)
	}

	// Create .gitignore if it doesn't exist
	gitignorePath := filepath.Join(s.PersistentDir, ".gitignore")
	if _, err := os.Stat(gitignorePath); os.IsNotExist(err) {
		os.WriteFile(gitignorePath, []byte("# DuckDB query results\n*.arrow\n"), 0o644)
	}

	// Copy file
	dst := filepath.Join(s.PersistentDir, filepath.Base(queryID)+".arrow")
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		os.Remove(dst)
		return err
	}

	// Remove from volatile dir — persistent copy is the source of truth now
	os.Remove(src)
	return nil
}

// Unpin moves an Arrow file from persistent dir back to volatile dir.
func (s *Spool) Unpin(queryID string) error {
	if s.PersistentDir == "" {
		return fmt.Errorf("persistent dir not configured")
	}
	src := filepath.Join(s.PersistentDir, filepath.Base(queryID)+".arrow")
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("pinned file not found: %w", err)
	}

	// Copy back to volatile
	dst := s.Path(queryID)
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		os.Remove(dst)
		return err
	}

	os.Remove(src)
	return nil
}

// IsPinned checks if a query result is in the persistent dir.
func (s *Spool) IsPinned(queryID string) bool {
	if s.PersistentDir == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(s.PersistentDir, filepath.Base(queryID)+".arrow"))
	return err == nil
}

// ListFiles returns all .arrow file names (without extension) in the spool dir.
func (s *Spool) ListFiles() ([]string, error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".arrow") {
			continue
		}
		ids = append(ids, strings.TrimSuffix(e.Name(), ".arrow"))
	}
	return ids, nil
}

// Cleanup deletes expired files and enforces disk size limit.
// Returns the number of files deleted.
func (s *Spool) Cleanup() (int, error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return 0, fmt.Errorf("read spool dir: %w", err)
	}

	type fileEntry struct {
		path    string
		size    int64
		modTime time.Time
	}

	now := time.Now()
	deleted := 0
	var live []fileEntry

	// Pass 1: delete expired files
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(s.Dir, entry.Name())

		if now.Sub(info.ModTime()) > s.TTL {
			os.Remove(path)
			deleted++
			continue
		}

		live = append(live, fileEntry{
			path:    path,
			size:    info.Size(),
			modTime: info.ModTime(),
		})
	}

	// Pass 2: enforce disk size limit (delete oldest first)
	sort.Slice(live, func(i, j int) bool {
		return live[i].modTime.After(live[j].modTime) // newest first
	})

	var totalSize int64
	for i, f := range live {
		totalSize += f.size
		if totalSize > s.MaxSize {
			// Delete this and all older files
			for j := i; j < len(live); j++ {
				os.Remove(live[j].path)
				deleted++
			}
			break
		}
	}

	return deleted, nil
}

// TotalSize returns the total size of all files in the spool dir.
func (s *Spool) TotalSize() (int64, error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total, nil
}

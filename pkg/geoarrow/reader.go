package geoarrow

import (
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Converter wraps an array.RecordReader and converts WKB geometry columns
// to native GeoArrow format on-the-fly. It implements array.RecordReader.
//
// The source schema should already be flattened (Struct/Map/Union expanded
// to top-level columns) before passing to the Converter. This ensures that
// geometry columns nested inside structs are accessible by column index.
//
// Multiple geometry columns per schema are supported — each is detected
// and converted independently (e.g., one column can be points, another polygons).
//
// Conversion is pipelined: up to bufferSize batches are converted ahead
// in parallel goroutines while the consumer reads previous results.
type Converter struct {
	refs    atomic.Int64
	source  array.RecordReader
	schema  *arrow.Schema
	geoCols []GeometryColumn
	mem     memory.Allocator

	// Pipeline state
	results chan batchResult
	current arrow.RecordBatch
	err     error

	// Per-column detected geometry types (shared across goroutines)
	geoTypes   map[int]GeoType // key = column index
	geoTypesMu sync.Mutex
}

type batchResult struct {
	rec arrow.RecordBatch
	err error
}

// Option configures the Converter.
type Option func(*converterConfig)

type converterConfig struct {
	bufferSize int
	mem        memory.Allocator
	geoCols    []GeometryColumn // override auto-detection
}

// WithBufferSize sets the number of batches to buffer ahead.
// This equals the number of goroutines doing conversion in parallel.
// Default: 1 (no parallelism, but still pipelined).
func WithBufferSize(n int) Option {
	return func(c *converterConfig) {
		if n > 0 {
			c.bufferSize = n
		}
	}
}

// WithAllocator sets the memory allocator for Arrow arrays.
func WithAllocator(mem memory.Allocator) Option {
	return func(c *converterConfig) {
		c.mem = mem
	}
}

// WithColumns overrides auto-detection and specifies which columns to convert.
func WithColumns(cols []GeometryColumn) Option {
	return func(c *converterConfig) {
		c.geoCols = cols
	}
}

// NewConverter creates a new WKB→GeoArrow converting RecordReader.
//
// It reads batches from source, detects WKB geometry columns (by
// ARROW:extension:name metadata), and converts them to native GeoArrow
// (MultiPoint/MultiLineString/MultiPolygon) columns.
//
// The source should already be flattened — if geometry is inside a struct,
// flatten first so the Converter can find it by column index.
//
// Multiple geometry columns are supported; each is converted independently.
//
// The conversion runs in a background pipeline with bufferSize goroutines.
// The output schema is determined after the first batch is converted
// (geometry column types are auto-detected from WKB content).
func NewConverter(source array.RecordReader, opts ...Option) *Converter {
	cfg := converterConfig{
		bufferSize: 1,
		mem:        memory.DefaultAllocator,
	}
	for _, o := range opts {
		o(&cfg)
	}

	geoCols := cfg.geoCols
	if len(geoCols) == 0 {
		geoCols = DetectGeometryColumns(source.Schema())
	}

	c := &Converter{
		source:   source,
		schema:   source.Schema(), // updated after first converted batch
		geoCols:  geoCols,
		mem:      cfg.mem,
		results:  make(chan batchResult, cfg.bufferSize),
		geoTypes: make(map[int]GeoType, len(geoCols)),
	}
	c.refs.Store(1)

	if len(geoCols) == 0 {
		// No geometry columns — pass through directly
		go c.passthrough()
	} else {
		go c.pipeline(cfg.bufferSize)
	}

	return c
}

// passthrough reads source batches without conversion.
func (c *Converter) passthrough() {
	defer close(c.results)
	for c.source.Next() {
		rec := c.source.RecordBatch()
		rec.Retain()
		c.results <- batchResult{rec: rec}
	}
	if err := c.source.Err(); err != nil {
		c.results <- batchResult{err: err}
	}
}

// pipeline reads source batches and converts them using worker goroutines.
// Batches are submitted in order; results are collected in order.
func (c *Converter) pipeline(workers int) {
	defer close(c.results)

	// Use a semaphore to limit concurrent conversions
	sem := make(chan struct{}, workers)

	// Ordered pipeline: read sequentially, convert concurrently, output in order.
	type job struct {
		ch chan batchResult
	}
	jobs := make(chan job, workers)

	// Collector: reads results in order
	var collectWg sync.WaitGroup
	collectWg.Add(1)
	go func() {
		defer collectWg.Done()
		for j := range jobs {
			result := <-j.ch
			c.results <- result
		}
	}()

	// Producer: reads from source, dispatches conversion
	for c.source.Next() {
		rec := c.source.RecordBatch()
		rec.Retain()

		ch := make(chan batchResult, 1)
		jobs <- job{ch: ch}

		sem <- struct{}{}
		go func(rec arrow.RecordBatch, ch chan batchResult) {
			defer func() { <-sem }()

			converted, err := c.convertOne(rec)
			rec.Release()

			if err != nil {
				ch <- batchResult{err: err}
				return
			}

			ch <- batchResult{rec: converted}
		}(rec, ch)
	}

	close(jobs)
	collectWg.Wait()

	if err := c.source.Err(); err != nil {
		c.results <- batchResult{err: err}
	}
}

// convertOne converts all geometry columns in a single batch.
// Each geometry column is converted independently with its own GeoType.
func (c *Converter) convertOne(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	result := rec
	result.Retain()

	for _, col := range c.geoCols {
		if col.Format != "WKB" {
			continue
		}

		// Get per-column geoType
		c.geoTypesMu.Lock()
		gt := c.geoTypes[col.Index]
		c.geoTypesMu.Unlock()

		converted, newGt, err := ConvertBatch(result, col, gt, c.mem)
		if err != nil {
			result.Release()
			return nil, err
		}

		if converted != result {
			result.Release()
			result = converted
		}

		// Update per-column geoType
		if newGt != GeoTypeUnknown && newGt != gt {
			c.geoTypesMu.Lock()
			if c.geoTypes[col.Index] == GeoTypeUnknown {
				c.geoTypes[col.Index] = newGt
			}
			c.geoTypesMu.Unlock()
		}
	}

	return result, nil
}

// ── array.RecordReader interface ──

func (c *Converter) Retain() {
	c.refs.Add(1)
}

func (c *Converter) Release() {
	if c.refs.Add(-1) == 0 {
		if c.current != nil {
			c.current.Release()
			c.current = nil
		}
		// Drain remaining results
		for r := range c.results {
			if r.rec != nil {
				r.rec.Release()
			}
		}
	}
}

func (c *Converter) Schema() *arrow.Schema {
	return c.schema
}

func (c *Converter) Next() bool {
	if c.current != nil {
		c.current.Release()
		c.current = nil
	}

	result, ok := <-c.results
	if !ok {
		return false
	}
	if result.err != nil {
		c.err = result.err
		return false
	}

	c.current = result.rec
	// Update schema from first converted batch
	if c.current != nil && c.current.Schema() != c.schema {
		c.schema = c.current.Schema()
	}
	return true
}

func (c *Converter) RecordBatch() arrow.RecordBatch {
	return c.current
}

// Record is a deprecated alias for RecordBatch.
func (c *Converter) Record() arrow.RecordBatch {
	return c.current
}

func (c *Converter) Err() error {
	return c.err
}

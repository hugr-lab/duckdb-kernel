package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/hugr-lab/duckdb-kernel/internal/kernel"
	"github.com/hugr-lab/duckdb-kernel/internal/session"
	"github.com/hugr-lab/duckdb-kernel/internal/spool"
)

func main() {
	connectionFile := flag.String("connection-file", "", "Path to Jupyter connection file")
	logFile := flag.String("log-file", "", "Path to log file (default: stderr)")
	basemaps := flag.String("basemaps", "", "JSON array of tile source configs, e.g. '[{\"name\":\"OSM\",\"url\":\"https://tile.openstreetmap.org/{z}/{x}/{y}.png\",\"type\":\"raster\"}]'")
	flag.Parse()

	// --log-file flag takes priority, then DUCKDB_KERNEL_LOG env var
	logPath := *logFile
	if logPath == "" {
		logPath = os.Getenv("DUCKDB_KERNEL_LOG")
	}
	if logPath != "" {
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: cannot open log file %s: %v (logging to stderr)\n", logPath, err)
		} else {
			defer f.Close()
			log.SetOutput(f)
		}
	}

	if *connectionFile == "" {
		fmt.Fprintln(os.Stderr, "Usage: duckdb-kernel --connection-file <path>")
		os.Exit(1)
	}

	// Parse connection file
	connInfo, err := parseConnectionFile(*connectionFile)
	if err != nil {
		log.Fatalf("Failed to parse connection file: %v", err)
	}

	// Create DuckDB connector
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		log.Fatalf("Failed to create DuckDB connector: %v", err)
	}
	defer connector.Close()

	// Determine session ID
	sessionID := os.Getenv("DUCKDB_SHARED_SESSION")
	if sessionID == "" {
		sessionID = uuid.New().String()
	}

	// Create session
	sess := session.NewSession(sessionID, connector)
	defer sess.Close()

	// Create result spool (flat dir, TTL-based cleanup)
	spoolCfg := spool.Config{
		Dir:     os.Getenv("DUCKDB_KERNEL_SPOOL_DIR"),
		TTL:     parseDuration(os.Getenv("DUCKDB_KERNEL_SPOOL_TTL"), spool.DefaultTTL),
		MaxSize: parseSize(os.Getenv("DUCKDB_KERNEL_SPOOL_MAX_SIZE"), spool.DefaultMaxSize),
	}
	sp, err := spool.NewSpool(spoolCfg)
	if err != nil {
		log.Printf("Warning: failed to create spool: %v (Arrow file output disabled)", err)
	}

	// Set persistent dir for pinned results (CWD/duckdb-results/)
	// Skip if CWD is root (e.g. VS Code launches kernel with CWD=/)
	if sp != nil {
		if cwd, err := os.Getwd(); err == nil && cwd != "/" {
			sp.PersistentDir = filepath.Join(cwd, "duckdb-results")
		}
	}

	// Create kernel
	k := kernel.NewKernel(connInfo, sess, sp)

	// Configure tile sources from --basemaps flag or DUCKDB_KERNEL_BASEMAPS env
	basemapsJSON := *basemaps
	if basemapsJSON == "" {
		basemapsJSON = os.Getenv("DUCKDB_KERNEL_BASEMAPS")
	}
	if basemapsJSON != "" {
		var tiles []kernel.TileSourceConfig
		if err := json.Unmarshal([]byte(basemapsJSON), &tiles); err != nil {
			log.Printf("Warning: failed to parse basemaps config: %v", err)
		} else {
			k.SetTileSources(tiles)
			log.Printf("Configured %d basemap tile source(s)", len(tiles))
		}
	}

	// Context cancelled on SIGINT, SIGTERM, or SIGHUP (VS Code may send SIGHUP on close).
	// The same context is passed to ZMQ sockets — cancellation unblocks Recv() calls.
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	// Wrap with a manual cancel so the watchdog can also cancel the context.
	ctx, cancel := context.WithCancel(sigCtx)
	defer cancel()

	// Watchdog: exit if parent process dies (e.g., VS Code crashed or closed).
	// This prevents orphaned kernel processes holding ZMQ ports.
	ppid := os.Getppid()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				if os.Getppid() != ppid {
					log.Println("Parent process exited, shutting down")
					cancel()
					return
				}
			}
		}
	}()

	// Start kernel
	log.Printf("Starting DuckDB Kernel (session: %s)", sessionID)
	if err := k.Start(ctx); err != nil {
		log.Fatalf("Kernel error: %v", err)
	}

	log.Println("DuckDB Kernel stopped")
}

func parseConnectionFile(path string) (*kernel.ConnectionInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read connection file: %w", err)
	}

	var info kernel.ConnectionInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("parse connection file: %w", err)
	}

	return &info, nil
}

// parseDuration parses a duration string, returning def on empty/error.
func parseDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Printf("invalid duration %q, using default %v", s, def)
		return def
	}
	return d
}

// parseSize parses a size string (e.g. "2g", "500m", "1073741824"), returning def on empty/error.
func parseSize(s string, def int64) int64 {
	if s == "" {
		return def
	}
	s = strings.TrimSpace(strings.ToLower(s))
	multiplier := int64(1)
	if strings.HasSuffix(s, "gb") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "gb")
	} else if strings.HasSuffix(s, "g") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "g")
	} else if strings.HasSuffix(s, "mb") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "mb")
	} else if strings.HasSuffix(s, "m") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "m")
	} else if strings.HasSuffix(s, "kb") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "kb")
	} else if strings.HasSuffix(s, "k") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "k")
	}
	n, err := fmt.Sscanf(s, "%d", new(int64))
	if err != nil || n == 0 {
		log.Printf("invalid size %q, using default %d", s, def)
		return def
	}
	var val int64
	fmt.Sscanf(s, "%d", &val)
	return val * multiplier
}

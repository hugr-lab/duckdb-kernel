package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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

	// Create result spool
	sp, err := spool.NewSpool(sessionID)
	if err != nil {
		log.Printf("Warning: failed to create spool: %v (Arrow file output disabled)", err)
	}

	// Create kernel
	k := kernel.NewKernel(connInfo, sess, sp)

	// Handle OS signals (including SIGHUP which VS Code may send)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-sigCh
		log.Println("Received shutdown signal")
		k.Shutdown()
		cancel()
	}()

	// Watchdog: exit if parent process dies (e.g., VS Code crashed or closed).
	// This prevents orphaned kernel processes.
	ppid := os.Getppid()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				if os.Getppid() != ppid {
					log.Println("Parent process exited, shutting down")
					k.Shutdown()
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

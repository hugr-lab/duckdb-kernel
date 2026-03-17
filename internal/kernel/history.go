package kernel

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

const maxHistorySize = 10

// HistoryEntry represents a single executed query in the history.
type HistoryEntry struct {
	Session int    `json:"session"`
	Line    int    `json:"line"`
	Source  string `json:"source"`
}

// History stores recent executed queries and persists them to a temp file.
type History struct {
	mu      sync.Mutex
	entries []HistoryEntry
	path    string
}

// NewHistory creates a history backed by a temp file for the given session.
func NewHistory(sessionID string) *History {
	path := filepath.Join(os.TempDir(), "duckdb-kernel", sessionID+".history.json")
	h := &History{path: path}
	h.load()
	return h
}

// Add appends a query to the history, keeping only the last maxHistorySize entries.
func (h *History) Add(execCount int, source string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.entries = append(h.entries, HistoryEntry{
		Session: 0,
		Line:    execCount,
		Source:  source,
	})
	if len(h.entries) > maxHistorySize {
		h.entries = h.entries[len(h.entries)-maxHistorySize:]
	}
	h.save()
}

// Entries returns all history entries as [session, line, source] tuples
// compatible with the Jupyter history_reply format.
func (h *History) Entries() [][]any {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := make([][]any, len(h.entries))
	for i, e := range h.entries {
		result[i] = []any{e.Session, e.Line, e.Source}
	}
	return result
}

func (h *History) load() {
	data, err := os.ReadFile(h.path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &h.entries)
	if len(h.entries) > maxHistorySize {
		h.entries = h.entries[len(h.entries)-maxHistorySize:]
	}
}

func (h *History) save() {
	data, err := json.Marshal(h.entries)
	if err != nil {
		return
	}
	_ = os.WriteFile(h.path, data, 0o644)
}

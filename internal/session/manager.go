package session

import (
	"sync"

	"github.com/duckdb/duckdb-go/v2"
)

// Manager manages shared sessions, keyed by session ID.
type Manager struct {
	sessions  map[string]*Session
	connector *duckdb.Connector
	mu        sync.RWMutex
}

// NewManager creates a new session manager.
func NewManager(connector *duckdb.Connector) *Manager {
	return &Manager{
		sessions:  make(map[string]*Session),
		connector: connector,
	}
}

// GetOrCreate returns an existing session or creates a new one.
func (m *Manager) GetOrCreate(sessionID string) *Session {
	m.mu.RLock()
	if sess, ok := m.sessions[sessionID]; ok {
		m.mu.RUnlock()
		return sess
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if sess, ok := m.sessions[sessionID]; ok {
		return sess
	}

	sess := NewSession(sessionID, m.connector)
	m.sessions[sessionID] = sess
	return sess
}

// Close closes all managed sessions.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sess := range m.sessions {
		sess.Close()
	}
	m.sessions = make(map[string]*Session)
}

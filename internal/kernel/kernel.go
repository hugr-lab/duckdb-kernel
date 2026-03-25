package kernel

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	zmq "github.com/go-zeromq/zmq4"
	"github.com/hugr-lab/duckdb-kernel/internal/engine"
	"github.com/hugr-lab/duckdb-kernel/internal/meta"
	"github.com/hugr-lab/duckdb-kernel/internal/session"
	"github.com/hugr-lab/duckdb-kernel/internal/spool"
)

// ConnectionInfo holds the parsed Jupyter connection file data.
type ConnectionInfo struct {
	Transport       string `json:"transport"`
	IP              string `json:"ip"`
	ShellPort       int    `json:"shell_port"`
	ControlPort     int    `json:"control_port"`
	IOPubPort       int    `json:"iopub_port"`
	StdinPort       int    `json:"stdin_port"`
	HBPort          int    `json:"hb_port"`
	Key             string `json:"key"`
	SignatureScheme string `json:"signature_scheme"`
}

// Endpoint returns the ZMQ endpoint string for a given port.
func (c *ConnectionInfo) Endpoint(port int) string {
	return fmt.Sprintf("%s://%s:%d", c.Transport, c.IP, port)
}

// TileSourceConfig holds a basemap tile source configuration.
type TileSourceConfig struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Type        string `json:"type"` // "raster", "vector", "tilejson"
	Attribution string `json:"attribution,omitempty"`
	MinZoom     int    `json:"min_zoom,omitempty"`
	MaxZoom     int    `json:"max_zoom,omitempty"`
}

// Kernel manages the Jupyter kernel lifecycle and ZMQ sockets.
type Kernel struct {
	connInfo     *ConnectionInfo
	session      *session.Session
	spool        *spool.Spool
	arrowServer  *ArrowServer
	metaRegistry *meta.Registry
	tileSources  []TileSourceConfig
	key          []byte

	shellSocket   zmq.Socket
	controlSocket zmq.Socket
	iopubSocket   zmq.Socket
	stdinSocket   zmq.Socket
	hbSocket      zmq.Socket

	history *History

	shutdown     chan struct{}
	shutdownOnce sync.Once
}

// NewKernel creates a new kernel with the given connection info, session, and spool.
func NewKernel(connInfo *ConnectionInfo, sess *session.Session, sp *spool.Spool) *Kernel {
	k := &Kernel{
		connInfo: connInfo,
		session:  sess,
		spool:    sp,
		key:      []byte(connInfo.Key),
		shutdown: make(chan struct{}),
	}
	k.metaRegistry = meta.NewRegistry(sess.Engine, sess.SetPreviewLimit, sp)
	historyDir := os.TempDir()
	if sp != nil {
		historyDir = sp.Dir
	}
	k.history = NewHistory(historyDir, sess.ID)
	return k
}

// SetTileSources configures basemap tile sources for the map plugin.
func (k *Kernel) SetTileSources(sources []TileSourceConfig) {
	k.tileSources = sources
}

// Start initializes ZMQ sockets and begins the message loop.
func (k *Kernel) Start(ctx context.Context) error {
	var err error

	// Create sockets
	k.hbSocket = zmq.NewRep(ctx)
	k.shellSocket = zmq.NewRouter(ctx)
	k.controlSocket = zmq.NewRouter(ctx)
	k.iopubSocket = zmq.NewPub(ctx)
	k.stdinSocket = zmq.NewRouter(ctx)

	// Listen on endpoints
	if err = k.hbSocket.Listen(k.connInfo.Endpoint(k.connInfo.HBPort)); err != nil {
		return fmt.Errorf("listen heartbeat: %w", err)
	}
	if err = k.shellSocket.Listen(k.connInfo.Endpoint(k.connInfo.ShellPort)); err != nil {
		return fmt.Errorf("listen shell: %w", err)
	}
	if err = k.controlSocket.Listen(k.connInfo.Endpoint(k.connInfo.ControlPort)); err != nil {
		return fmt.Errorf("listen control: %w", err)
	}
	if err = k.iopubSocket.Listen(k.connInfo.Endpoint(k.connInfo.IOPubPort)); err != nil {
		return fmt.Errorf("listen iopub: %w", err)
	}
	if err = k.stdinSocket.Listen(k.connInfo.Endpoint(k.connInfo.StdinPort)); err != nil {
		return fmt.Errorf("listen stdin: %w", err)
	}

	// Start Arrow HTTP server for direct file serving and introspection.
	// Skip if spool proxy handles serving (JupyterHub/Docker).
	if k.spool != nil && os.Getenv("HUGR_SPOOL_PROXY") == "" {
		introspector := engine.NewIntrospector(k.session.Engine, k.session.ID)
		as, err := NewArrowServer(k.spool, introspector)
		if err != nil {
			log.Printf("Warning: failed to start Arrow HTTP server: %v", err)
		} else {
			k.arrowServer = as
		}
	}

	log.Printf("DuckDB Kernel started on %s://%s", k.connInfo.Transport, k.connInfo.IP)

	// Start heartbeat goroutine
	go k.heartbeatLoop(ctx)

	// Start shell handler
	go k.shellLoop(ctx)

	// Start control handler
	go k.controlLoop(ctx)

	// Wait for shutdown
	select {
	case <-ctx.Done():
	case <-k.shutdown:
	}

	return k.close()
}

// Shutdown signals the kernel to stop.
func (k *Kernel) Shutdown() {
	k.shutdownOnce.Do(func() {
		close(k.shutdown)
	})
}

func (k *Kernel) close() error {
	if k.arrowServer != nil {
		k.arrowServer.Close()
	}
	// Spool files are preserved for TTL-based recovery.
	// Cleanup happens on next kernel start.
	k.hbSocket.Close()
	k.shellSocket.Close()
	k.controlSocket.Close()
	k.iopubSocket.Close()
	k.stdinSocket.Close()
	return nil
}

func (k *Kernel) heartbeatLoop(ctx context.Context) {
	for {
		msg, err := k.hbSocket.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-k.shutdown:
				return
			default:
				log.Printf("heartbeat recv error: %v", err)
				return
			}
		}
		if err := k.hbSocket.Send(msg); err != nil {
			log.Printf("heartbeat send error: %v", err)
			return
		}
	}
}

func (k *Kernel) shellLoop(ctx context.Context) {
	for {
		zmqMsg, err := k.shellSocket.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-k.shutdown:
				return
			default:
				log.Printf("shell recv error: %v", err)
				return
			}
		}

		if !VerifySignature(k.key, zmqMsg.Frames) {
			log.Printf("shell: invalid signature, dropping message")
			continue
		}

		msg, err := Deserialize(zmqMsg.Frames)
		if err != nil {
			log.Printf("shell deserialize error: %v", err)
			continue
		}

		k.handleShellMessage(ctx, msg)
	}
}

func (k *Kernel) controlLoop(ctx context.Context) {
	for {
		zmqMsg, err := k.controlSocket.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-k.shutdown:
				return
			default:
				log.Printf("control recv error: %v", err)
				return
			}
		}

		if !VerifySignature(k.key, zmqMsg.Frames) {
			log.Printf("control: invalid signature, dropping message")
			continue
		}

		msg, err := Deserialize(zmqMsg.Frames)
		if err != nil {
			log.Printf("control deserialize error: %v", err)
			continue
		}

		if msg.Header.MsgType == "shutdown_request" {
			k.handleShutdownRequest(msg)
		}
	}
}

func (k *Kernel) sendMessage(socket zmq.Socket, msg *Message) error {
	frames, err := msg.Serialize(k.key)
	if err != nil {
		return fmt.Errorf("serialize: %w", err)
	}
	zmqMsg := zmq.NewMsgFrom(frames...)
	return socket.Send(zmqMsg)
}

func (k *Kernel) publishStatus(parent *Message, status string) {
	msg := NewMessage(parent, "status")
	msg.Content["execution_state"] = status
	if err := k.sendMessage(k.iopubSocket, msg); err != nil {
		log.Printf("publish status error: %v", err)
	}
}

package kernel

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
	"github.com/hugr-lab/duckdb-kernel/internal/meta"
	"github.com/hugr-lab/duckdb-kernel/internal/renderer"
)

const (
	KernelVersion = "0.1.0"
)

func (k *Kernel) handleShellMessage(ctx context.Context, msg *Message) {
	k.publishStatus(msg, "busy")
	defer k.publishStatus(msg, "idle")

	switch msg.Header.MsgType {
	case "kernel_info_request":
		k.handleKernelInfoRequest(msg)
	case "execute_request":
		k.handleExecuteRequest(ctx, msg)
	case "is_complete_request":
		k.handleIsCompleteRequest(msg)
	case "complete_request":
		k.handleCompleteRequest(msg)
	case "inspect_request":
		k.handleInspectRequest(msg)
	default:
		log.Printf("unhandled shell message type: %s", msg.Header.MsgType)
	}
}

func (k *Kernel) handleKernelInfoRequest(msg *Message) {
	reply := NewMessage(msg, "kernel_info_reply")
	reply.Content = map[string]any{
		"protocol_version": ProtocolVersion,
		"implementation":   "duckdb-kernel",
		"implementation_version": KernelVersion,
		"language_info": map[string]any{
			"name":           "sql",
			"version":        "",
			"mimetype":       "application/sql",
			"file_extension": ".sql",
		},
		"banner": fmt.Sprintf("DuckDB Kernel v%s", KernelVersion),
		"status": "ok",
	}
	if err := k.sendMessage(k.shellSocket, reply); err != nil {
		log.Printf("send kernel_info_reply error: %v", err)
	}
}

func (k *Kernel) handleExecuteRequest(ctx context.Context, msg *Message) {
	code, _ := msg.Content["code"].(string)
	code = strings.TrimSpace(code)

	execCount := k.session.ExecutionCount()

	// Publish execute_input on iopub
	inputMsg := NewMessage(msg, "execute_input")
	inputMsg.Content = map[string]any{
		"code":            code,
		"execution_count": execCount,
	}
	if err := k.sendMessage(k.iopubSocket, inputMsg); err != nil {
		log.Printf("send execute_input error: %v", err)
	}

	// Empty input — no output
	if code == "" {
		reply := NewMessage(msg, "execute_reply")
		reply.Content = map[string]any{
			"status":          "ok",
			"execution_count": execCount,
		}
		if err := k.sendMessage(k.shellSocket, reply); err != nil {
			log.Printf("send execute_reply error: %v", err)
		}
		return
	}

	// Check for meta command
	if meta.IsMeta(code) {
		output, err := k.metaRegistry.Dispatch(ctx, code)
		if err != nil {
			k.sendExecuteError(msg, execCount, err)
			return
		}
		if output != "" {
			displayMsg := NewMessage(msg, "display_data")
			displayMsg.Content = map[string]any{
				"data": map[string]any{
					"text/plain": output,
				},
				"metadata":  map[string]any{},
				"transient": map[string]any{},
			}
			if err := k.sendMessage(k.iopubSocket, displayMsg); err != nil {
				log.Printf("send display_data error: %v", err)
			}
		}
		reply := NewMessage(msg, "execute_reply")
		reply.Content = map[string]any{
			"status":          "ok",
			"execution_count": execCount,
		}
		if err := k.sendMessage(k.shellSocket, reply); err != nil {
			log.Printf("send execute_reply error: %v", err)
		}
		return
	}

	// Execute SQL
	result, err := k.session.Execute(ctx, code)
	if err != nil {
		k.sendExecuteError(msg, execCount, err)
		return
	}
	defer result.Release()

	// Write Arrow IPC file
	if k.spool != nil && len(result.Records) > 0 {
		queryID := uuid.New().String()
		result.QueryID = queryID
		if err := k.spool.Write(queryID, result.Records); err != nil {
			log.Printf("spool write error: %v", err)
		}
		if err := k.spool.Cleanup(); err != nil {
			log.Printf("spool cleanup error: %v", err)
		}
	}

	// Send display_data if there are results
	if len(result.Columns) > 0 {
		columns := make([]string, len(result.Columns))
		for i, col := range result.Columns {
			columns[i] = col.Name
		}

		tableText := renderer.RenderTable(columns, result.Rows)

		if result.TotalRows > int64(len(result.Rows)) {
			tableText += fmt.Sprintf("\n\n(%d rows total, showing %d)", result.TotalRows, len(result.Rows))
		}

		displayMsg := NewMessage(msg, "display_data")
		displayMsg.Content = map[string]any{
			"data": map[string]any{
				"text/plain": tableText,
			},
			"metadata":  map[string]any{},
			"transient": map[string]any{},
		}
		if err := k.sendMessage(k.iopubSocket, displayMsg); err != nil {
			log.Printf("send display_data error: %v", err)
		}
	}

	// Send execute_reply
	reply := NewMessage(msg, "execute_reply")
	reply.Content = map[string]any{
		"status":          "ok",
		"execution_count": k.session.ExecutionCount(),
	}
	if err := k.sendMessage(k.shellSocket, reply); err != nil {
		log.Printf("send execute_reply error: %v", err)
	}
}

func (k *Kernel) sendExecuteError(msg *Message, execCount int, execErr error) {
	errMsg := NewMessage(msg, "error")
	errMsg.Content = map[string]any{
		"ename":     "SQLError",
		"evalue":    execErr.Error(),
		"traceback": []string{execErr.Error()},
	}
	if err := k.sendMessage(k.iopubSocket, errMsg); err != nil {
		log.Printf("send error error: %v", err)
	}

	reply := NewMessage(msg, "execute_reply")
	reply.Content = map[string]any{
		"status":          "error",
		"execution_count": execCount,
		"ename":           "SQLError",
		"evalue":          execErr.Error(),
		"traceback":       []string{execErr.Error()},
	}
	if err := k.sendMessage(k.shellSocket, reply); err != nil {
		log.Printf("send execute_reply error: %v", err)
	}
}

func (k *Kernel) handleIsCompleteRequest(msg *Message) {
	reply := NewMessage(msg, "is_complete_reply")
	reply.Content = map[string]any{
		"status": "complete",
	}
	if err := k.sendMessage(k.shellSocket, reply); err != nil {
		log.Printf("send is_complete_reply error: %v", err)
	}
}

func (k *Kernel) handleCompleteRequest(msg *Message) {
	reply := NewMessage(msg, "complete_reply")
	reply.Content = map[string]any{
		"status":       "ok",
		"matches":      []string{},
		"cursor_start": 0,
		"cursor_end":   0,
		"metadata":     map[string]any{},
	}
	if err := k.sendMessage(k.shellSocket, reply); err != nil {
		log.Printf("send complete_reply error: %v", err)
	}
}

func (k *Kernel) handleInspectRequest(msg *Message) {
	reply := NewMessage(msg, "inspect_reply")
	reply.Content = map[string]any{
		"status":   "ok",
		"found":    false,
		"data":     map[string]any{},
		"metadata": map[string]any{},
	}
	if err := k.sendMessage(k.shellSocket, reply); err != nil {
		log.Printf("send inspect_reply error: %v", err)
	}
}

func (k *Kernel) handleShutdownRequest(msg *Message) {
	restart, _ := msg.Content["restart"].(bool)

	reply := NewMessage(msg, "shutdown_reply")
	reply.Content = map[string]any{
		"status":  "ok",
		"restart": restart,
	}
	if err := k.sendMessage(k.controlSocket, reply); err != nil {
		log.Printf("send shutdown_reply error: %v", err)
	}

	k.Shutdown()
}

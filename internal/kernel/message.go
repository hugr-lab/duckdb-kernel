package kernel

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

const (
	Delimiter      = "<IDS|MSG>"
	ProtocolVersion = "5.0"
)

// Header represents a Jupyter message header.
type Header struct {
	MsgID    string `json:"msg_id"`
	Session  string `json:"session"`
	Username string `json:"username"`
	Date     string `json:"date"`
	MsgType  string `json:"msg_type"`
	Version  string `json:"version"`
}

// Message represents a complete Jupyter wire protocol message.
type Message struct {
	Identities   [][]byte
	Header       Header
	ParentHeader Header
	Metadata     map[string]any
	Content      map[string]any
}

// NewMessage creates a new message as a reply to the given parent.
func NewMessage(parent *Message, msgType string) *Message {
	return &Message{
		Identities: parent.Identities,
		Header: Header{
			MsgID:    uuid.New().String(),
			Session:  parent.Header.Session,
			Username: parent.Header.Username,
			Date:     "",
			MsgType:  msgType,
			Version:  ProtocolVersion,
		},
		ParentHeader: parent.Header,
		Metadata:     make(map[string]any),
		Content:      make(map[string]any),
	}
}

// Sign computes the HMAC-SHA256 signature for the message parts.
func Sign(key []byte, header, parentHeader, metadata, content []byte) string {
	if len(key) == 0 {
		return ""
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(header)
	mac.Write(parentHeader)
	mac.Write(metadata)
	mac.Write(content)
	return hex.EncodeToString(mac.Sum(nil))
}

// Serialize converts the message to wire format frames.
func (m *Message) Serialize(key []byte) ([][]byte, error) {
	header, err := json.Marshal(m.Header)
	if err != nil {
		return nil, fmt.Errorf("marshal header: %w", err)
	}
	parentHeader, err := json.Marshal(m.ParentHeader)
	if err != nil {
		return nil, fmt.Errorf("marshal parent_header: %w", err)
	}
	metadata, err := json.Marshal(m.Metadata)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	content, err := json.Marshal(m.Content)
	if err != nil {
		return nil, fmt.Errorf("marshal content: %w", err)
	}

	sig := Sign(key, header, parentHeader, metadata, content)

	frames := make([][]byte, 0, len(m.Identities)+5)
	frames = append(frames, m.Identities...)
	frames = append(frames, []byte(Delimiter))
	frames = append(frames, []byte(sig))
	frames = append(frames, header)
	frames = append(frames, parentHeader)
	frames = append(frames, metadata)
	frames = append(frames, content)

	return frames, nil
}

// Deserialize parses wire format frames into a Message.
func Deserialize(frames [][]byte) (*Message, error) {
	// Find delimiter position
	delimIdx := -1
	for i, f := range frames {
		if string(f) == Delimiter {
			delimIdx = i
			break
		}
	}
	if delimIdx < 0 {
		return nil, fmt.Errorf("delimiter %q not found in message", Delimiter)
	}

	// Need at least: delimiter + signature + header + parent_header + metadata + content
	if len(frames) < delimIdx+6 {
		return nil, fmt.Errorf("message too short: expected at least %d frames, got %d", delimIdx+6, len(frames))
	}

	msg := &Message{
		Identities: frames[:delimIdx],
		Metadata:   make(map[string]any),
		Content:    make(map[string]any),
	}

	// frames[delimIdx] is the delimiter
	// frames[delimIdx+1] is the signature (verified externally if needed)
	headerBytes := frames[delimIdx+2]
	parentHeaderBytes := frames[delimIdx+3]
	metadataBytes := frames[delimIdx+4]
	contentBytes := frames[delimIdx+5]

	if err := json.Unmarshal(headerBytes, &msg.Header); err != nil {
		return nil, fmt.Errorf("unmarshal header: %w", err)
	}
	if err := json.Unmarshal(parentHeaderBytes, &msg.ParentHeader); err != nil {
		return nil, fmt.Errorf("unmarshal parent_header: %w", err)
	}
	if err := json.Unmarshal(metadataBytes, &msg.Metadata); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	if err := json.Unmarshal(contentBytes, &msg.Content); err != nil {
		return nil, fmt.Errorf("unmarshal content: %w", err)
	}

	return msg, nil
}

// VerifySignature checks the HMAC signature of incoming message frames.
func VerifySignature(key []byte, frames [][]byte) bool {
	if len(key) == 0 {
		return true
	}

	delimIdx := -1
	for i, f := range frames {
		if string(f) == Delimiter {
			delimIdx = i
			break
		}
	}
	if delimIdx < 0 || len(frames) < delimIdx+6 {
		return false
	}

	sig := string(frames[delimIdx+1])
	expected := Sign(key,
		frames[delimIdx+2],
		frames[delimIdx+3],
		frames[delimIdx+4],
		frames[delimIdx+5],
	)

	return hmac.Equal([]byte(sig), []byte(expected))
}

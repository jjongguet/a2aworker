// a2a.go — A2A v1.0-구조의 프로토콜 타입. 스펙에서 검증된 부분만 반영:
//   - JSON-RPC 2.0 기반, 메서드명 PascalCase (SendMessage/GetTask/ListTasks/CancelTask)
//   - AgentCard at /.well-known/agent-card.json, AgentInterface.tenant 필드
//   - Task 상태머신: submitted/working/input-required/completed/failed/canceled/rejected/auth-required
//
// 토이 단순화: Part는 Text/Data 두 종만, streaming(SubscribeToTask)/push-config는 미구현.
package main

import "encoding/json"

// ---- Agent Card ----

type AgentCard struct {
	Name               string                    `json:"name"`
	Description        string                    `json:"description"`
	Version            string                    `json:"version"`
	Capabilities       Capabilities              `json:"capabilities"`
	DefaultInputModes  []string                  `json:"defaultInputModes"`
	DefaultOutputModes []string                  `json:"defaultOutputModes"`
	AgentInterfaces    []AgentInterface          `json:"agentInterfaces"`
	Skills             []Skill                   `json:"skills"`
	SecuritySchemes    map[string]SecurityScheme `json:"securitySchemes"`
	PreferredTransport string                    `json:"preferredTransport"`
}

type Capabilities struct {
	Streaming              bool `json:"streaming"`
	StateTransitionHistory bool `json:"stateTransitionHistory"`
}

// AgentInterface.tenant — v1.0 스펙 §4.4.6: "An opaque string used for routing
// requests to a specific agent or tenant when multiple agents are served behind
// a single A2A endpoint."
type AgentInterface struct {
	Name   string `json:"name"`
	URL    string `json:"url"`
	Tenant string `json:"tenant"`
}

type SecurityScheme struct {
	Type   string `json:"type"`   // "http"
	Scheme string `json:"scheme"` // "bearer"
}

type Skill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags,omitempty"`
}

// ---- Messages / Tasks ----

type Message struct {
	Role     string         `json:"role"` // "user" | "agent"
	Parts    []Part         `json:"parts"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type Part struct {
	Kind string         `json:"kind"` // "text" | "data"
	Text string         `json:"text,omitempty"`
	Data map[string]any `json:"data,omitempty"`
}

func TextPart(s string) Part { return Part{Kind: "text", Text: s} }

type TaskStatus struct {
	State   string   `json:"state"`
	Message *Message `json:"message,omitempty"`
}

type Artifact struct {
	Name     string         `json:"name"`
	Parts    []Part         `json:"parts"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type Task struct {
	ID        string         `json:"id"`
	ContextID string         `json:"contextId"`
	Status    TaskStatus     `json:"status"`
	Artifacts []Artifact     `json:"artifacts,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	History   []TaskStatus   `json:"history,omitempty"` // capabilities.stateTransitionHistory
}

// Task states — v1.0 이름 그대로.
const (
	StateSubmitted     = "submitted"
	StateWorking       = "working"
	StateInputRequired = "input-required"
	StateCompleted     = "completed"
	StateFailed        = "failed"
	StateCanceled      = "canceled"
	StateRejected      = "rejected"
	StateAuthRequired  = "auth-required"
)

// ---- JSON-RPC 2.0 ----

type RPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type RPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// A2A 표준/준표준 오류 코드 (-32009 VersionNotSupported는 스펙 지정).
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeTaskNotFound   = -32002
	CodeUnsupportedOp  = -32004
	CodeVersionNotSup  = -32009
)

// MessageSendParams — v1.0 요청 객체는 tenant 필드를 운반한다.
type MessageSendParams struct {
	Message  Message        `json:"message"`
	Tenant   string         `json:"tenant"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type GetTaskParams struct {
	TaskID string `json:"taskId"`
	Tenant string `json:"tenant"`
}

type ListTasksParams struct {
	Tenant string `json:"tenant"`
}

type CancelTaskParams struct {
	TaskID string `json:"taskId"`
	Tenant string `json:"tenant"`
}

// a2aclient.go — A2A 클라이언트(원격 엔드포인트 호출 측). federation의 caller 반쪽:
// 다른 위치의 A2A 엔드포인트(bridge 등)를 표준 헤더·엔벨로프로 호출한다.
//
// 규칙:
//   - JSON-RPC 2.0 엔벨로프(RPCRequest/RPCResponse 재사용), 메서드 PascalCase
//   - 헤더: Content-Type: application/json, A2A-Version: 1.0, Authorization: Bearer
//   - 서버 응답의 RPCError는 (nil, *RPCError, nil) — 프로토콜 오류와 전송 오류(error) 구분
//   - SendMessage/GetTask의 result는 Task로 역직렬화(id+status.state 검증).
//     원격이 Message 등을 반환하면 빈 Task 대신 명시적 error.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// TerminalStates — WaitTerminal의 폴링 종료 조건.
// input-required는 터미널이 아니다(계속 폴링, caller는 ctx 취소로 중단).
// auth-required도 스펙 상태머신에서 종료 상태가 아니므로 폴링 대상으로 둔다.
var TerminalStates = map[string]bool{
	StateCompleted: true,
	StateFailed:    true,
	StateCanceled:  true,
	StateRejected:  true,
}

// A2AClient — 원격 A2A 엔드포인트 1곳에 대응. 값 리터럴로 생성:
//
//	c := &A2AClient{BaseURL: "http://127.0.0.1:9600", Token: tok}
type A2AClient struct {
	BaseURL string       // 예: http://127.0.0.1:9600 (trailing slash 허용)
	Token   string       // Bearer
	HTTP    *http.Client // nil이면 기본 클라이언트
}

// FetchCard — GET /.well-known/agent-card.json. tenant 라우팅 표(AgentInterfaces) 획득.
func (c *A2AClient) FetchCard(ctx context.Context) (*AgentCard, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/.well-known/agent-card.json"), nil)
	if err != nil {
		return nil, fmt.Errorf("a2a card: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("a2a card: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("a2a card: http %s", resp.Status)
	}
	var card AgentCard
	if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
		return nil, fmt.Errorf("a2a card: decode: %w", err)
	}
	return &card, nil
}

// SendMessage — message/send. 정책 판정 결과도 Task 상태(rejected/input-required)로
// 돌아오므로, 프로토콜 오류가 아니면 항상 Task를 반환한다.
func (c *A2AClient) SendMessage(ctx context.Context, p MessageSendParams) (*Task, *RPCError, error) {
	r, err := c.call(ctx, "SendMessage", p)
	if err != nil {
		return nil, nil, err
	}
	if r.Error != nil {
		return nil, r.Error, nil
	}
	t, err := taskFromResult("SendMessage", r)
	if err != nil {
		return nil, nil, err
	}
	return t, nil, nil
}

// GetTask — tasks/get. 존재하지 않는 taskId는 RPCError -32002로 돌아온다.
func (c *A2AClient) GetTask(ctx context.Context, taskID string) (*Task, *RPCError, error) {
	r, err := c.call(ctx, "GetTask", GetTaskParams{TaskID: taskID})
	if err != nil {
		return nil, nil, err
	}
	if r.Error != nil {
		return nil, r.Error, nil
	}
	t, err := taskFromResult("GetTask", r)
	if err != nil {
		return nil, nil, err
	}
	return t, nil, nil
}

// WaitTerminal — TerminalStates 도달까지 GetTask 폴링(첫 조회는 즉시).
// input-required 등 비터미널 상태에서는 계속 폴링하며, caller는 ctx 취소로 중단
// 한다(만료 시 그 에러를 그대로 반환). poll이 0 이하면 250ms로 방어(busy-loop 금지).
func (c *A2AClient) WaitTerminal(ctx context.Context, taskID string, poll time.Duration) (*Task, error) {
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}
	for {
		task, rpcErr, err := c.GetTask(ctx, taskID)
		if err != nil {
			return nil, err
		}
		if rpcErr != nil {
			return nil, fmt.Errorf("a2a GetTask(%s): rpc %d %s", taskID, rpcErr.Code, rpcErr.Message)
		}
		if TerminalStates[task.Status.State] {
			return task, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(poll):
		}
	}
}

// ---- 내부 ----

// call — JSON-RPC 2.0 POST 1건. HTTP 상태와 무관하게 본문이 RPCResponse.Error를
// 싣고 있으면 그대로 반환해 호출자가 프로토콜 오류로 판별하게 한다(예: 401 본문).
func (c *A2AClient) call(ctx context.Context, method string, params any) (*RPCResponse, error) {
	pb, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("a2a %s: params: %w", method, err)
	}
	body, err := json.Marshal(RPCRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage("1"),
		Method:  method,
		Params:  pb,
	})
	if err != nil {
		return nil, fmt.Errorf("a2a %s: request: %w", method, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url("/"), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("a2a %s: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("A2A-Version", "1.0")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("a2a %s: %w", method, err)
	}
	defer resp.Body.Close()
	var out RPCResponse
	derr := json.NewDecoder(resp.Body).Decode(&out)
	if derr != nil {
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("a2a %s: http %s (non-JSON body)", method, resp.Status)
		}
		return nil, fmt.Errorf("a2a %s: decode: %w", method, derr)
	}
	if resp.StatusCode != http.StatusOK && out.Error == nil {
		return nil, fmt.Errorf("a2a %s: http %s", method, resp.Status)
	}
	return &out, nil
}

// taskFromResult — result를 Task로 역직렬화. id 또는 status.state이 없으면
// (원격이 Message 등을 반환) 에러: 조용히 빈 Task를 돌려주는 것보다 명시적 실패.
func taskFromResult(method string, r *RPCResponse) (*Task, error) {
	raw, err := json.Marshal(r.Result)
	if err != nil {
		return nil, fmt.Errorf("a2a %s: result: %w", method, err)
	}
	var t Task
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, fmt.Errorf("a2a %s: result is not a Task: %w", method, err)
	}
	if t.ID == "" || t.Status.State == "" {
		return nil, fmt.Errorf("a2a %s: result is not a Task (missing id/status.state): %.200s", method, raw)
	}
	return &t, nil
}

// url — BaseURL 정규화(trailing slash 제거) 후 경로 결합.
func (c *A2AClient) url(path string) string {
	return strings.TrimRight(c.BaseURL, "/") + path
}

func (c *A2AClient) do(req *http.Request) (*http.Response, error) {
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	return hc.Do(req)
}

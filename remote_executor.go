// remote_executor.go — 크로스호스트 federation 실행기. "서로 다른 위치(호스트)의
// 에이전트 상호 위임"을 구현한다: Routes[profile] → 원격 브리지의 A2A 엔드포인트로
// 실행을 위임하고, 원격이 시행한 정책 결과(카탈로그/elevated/depth/ask_owner)를
// 로컬 Executor 계약으로 매핑만 한다.
//
//	클라이언트 ──HMAC──▶ 브리지 A ──FedToken+X-A2A-Caller──▶ 브리지 B(원격)
//	                    RemoteExecutor                        합성 claims + 정책 그대로
//
// 상태 매핑: completed → artifact 텍스트 / rejected·failed·canceled → *ErrRemoteTerminal
// / input-required → *ErrInputRequired / submitted·working → WaitTerminal 후 재매핑.
package main

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// ErrInputRequired — 원격 태스크가 input-required(예: sensitive 스킬의 ask_owner)로
// 멈춤. bridge는 이 에러를 잡아 로컬 Task를 input-required로 옮기고 remoteTaskId/
// remoteBaseURL을 기록해, 이후 소유주 결정을 원격으로 중계한다.
type ErrInputRequired struct {
	TaskID   string
	Question string
	BaseURL  string // 원격 브리지 URL (owner_decision 중계 대상)
}

func (e *ErrInputRequired) Error() string {
	return fmt.Sprintf("remote task %s requires input: %s", e.TaskID, e.Question)
}

// ErrRemoteTerminal — 원격 태스크가 거절/실패/취소로 종료. State로 원격 종료 상태를
// 구분해 담는다(bridge는 rejected를 로컬 rejected로 전파).
type ErrRemoteTerminal struct {
	TaskID string
	State  string // StateRejected | StateFailed | StateCanceled
	Reason string
}

func (e *ErrRemoteTerminal) Error() string {
	return fmt.Sprintf("federation task %s %s: %s", e.TaskID, e.State, e.Reason)
}

// RemoteExecutor — profile(=tenant)을 원격 브리지 BaseURL로 라우팅하는 실행기.
type RemoteExecutor struct {
	Routes    map[string]string            // profile → 원격 BaseURL
	FedToken  string                       // 원격이 수락할 federation Bearer
	Poll      time.Duration                // WaitTerminal 폴링 주기 (기본 20ms)
	Timeout   time.Duration                // 원격 실행 전체 예산 (기본 5s)
	Caller    string                       // X-A2A-Caller/위임 metadata의 caller 값 (기본 "bridge")
	NewClient func(base string) *A2AClient // 테스트 주입용; nil이면 기본 생성
}

// clientFor — 라우트별 클라이언트. 기본 생성 시 X-A2A-Caller 헤더(원격 인바운드가
// 합성 claims Sub "federation:<caller>"를 만드는 값)를 transport에서 싣는다.
func (e *RemoteExecutor) clientFor(base string) *A2AClient {
	if e.NewClient != nil {
		return e.NewClient(base)
	}
	return &A2AClient{BaseURL: base, Token: e.FedToken, HTTP: &http.Client{
		Transport: &callerTransport{caller: e.caller()},
	}}
}

func (e *RemoteExecutor) caller() string {
	if e.Caller != "" {
		return e.Caller
	}
	return "bridge"
}

func (e *RemoteExecutor) pollEvery() time.Duration {
	if e.Poll > 0 {
		return e.Poll
	}
	return 20 * time.Millisecond
}

func (e *RemoteExecutor) budget() time.Duration {
	if e.Timeout > 0 {
		return e.Timeout
	}
	return 5 * time.Second
}

// callerTransport — 나가는 federation 호출에 X-A2A-Caller 헤더를 붙인다.
type callerTransport struct {
	caller string
	next   http.RoundTripper // nil → http.DefaultTransport (테스트 녹화 주입용)
}

func (t *callerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Set("X-A2A-Caller", t.caller)
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(r)
}

// Execute — Routes 조회 → 원격 SendMessage(Tenant=profile, Message=msg,
// Metadata={skill}) → 원격 Task 상태 매핑. traceID가 있으면 위임 metadata에
// 전파해 원격 홉까지 동일 trace로 상관한다(비어 있으면 새 trace 생성).
func (e *RemoteExecutor) Execute(profile, skill string, msg Message, traceID string) (string, error) {
	base, ok := e.Routes[profile]
	if !ok {
		return "", fmt.Errorf("federation: no route for profile %q", profile)
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.budget())
	defer cancel()

	out := msg
	out.Metadata = withDelegation(msg.Metadata, e.caller(), skill, traceID)

	task, rpcErr, err := e.clientFor(base).SendMessage(ctx, MessageSendParams{
		Tenant:   profile,
		Message:  out,
		Metadata: map[string]any{"skill": skill},
	})
	if err != nil {
		return "", fmt.Errorf("federation send to %s: %w", base, err)
	}
	if rpcErr != nil {
		// 원격이 RPC 수준에서 거절(unknown tenant 등) — task 없이 거절 사유만 옴.
		return "", &ErrRemoteTerminal{State: StateRejected,
			Reason: fmt.Sprintf("remote RPC error [%d] %s", rpcErr.Code, rpcErr.Message)}
	}
	for task != nil {
		switch task.Status.State {
		case StateCompleted:
			return taskText(task), nil
		case StateInputRequired:
			q := ""
			if task.Status.Message != nil {
				q = textOf(*task.Status.Message)
			}
			return "", &ErrInputRequired{TaskID: task.ID, Question: q, BaseURL: base}
		case StateFailed, StateCanceled, StateRejected:
			why := ""
			if task.Status.Message != nil {
				why = textOf(*task.Status.Message)
			}
			return "", &ErrRemoteTerminal{TaskID: task.ID, State: task.Status.State, Reason: why}
		default: // submitted·working: 종료 상태까지 대기 후 재매핑
			task, err = e.clientFor(base).WaitTerminal(ctx, task.ID, e.pollEvery())
			if err != nil {
				return "", fmt.Errorf("federation wait for %s: %w", base, err)
			}
		}
	}
	return "", fmt.Errorf("federation: nil task from %s", base)
}

// taskText — 원격 결과 텍스트: artifact 우선, 없으면 상태 메시지.
func taskText(t *Task) string {
	for i := range t.Artifacts {
		for _, p := range t.Artifacts[i].Parts {
			if p.Kind == "text" && p.Text != "" {
				return p.Text
			}
		}
	}
	if t.Status.Message != nil {
		return textOf(*t.Status.Message)
	}
	return ""
}

// withDelegation — 나가는 Message에 위임 추적 metadata 부착({caller, skill, depth,
// traceId}). depth는 "이 요청이 도착하기까지의 위임 홉 수"다: 새 로컬 요청이면 0,
// 인바운드 federation이면 그 delegation.depth+1. 원격 인바운드는 depth+1개의 합성
// 액터로 체인을 이어 depth 홉 증가(무한 페더레이션)를 방어한다.
// traceID(로컬 Task의 위임 trace)가 우선하고, 없으면 기존 delegation.traceId 상속,
// 둘 다 없으면 새로 생성한다 — 로컬↔원격이 같은 논리 작업으로 상관된다.
func withDelegation(src map[string]any, caller, skill, traceID string) map[string]any {
	m := map[string]any{}
	for k, v := range src {
		m[k] = v
	}
	depth := 0
	trace := newTraceID() // 새 체인이면 새 trace, 상속 우선: traceID 인자 → 기존 delegation.traceId
	if traceID != "" {
		trace = traceID
	}
	if d, ok := m["delegation"].(map[string]any); ok {
		delete(m, "delegation") // 상위 체인 정보는 이 홉 기준으로 교체
		if n, ok := d["depth"].(float64); ok && n >= 0 {
			depth = int(n) + 1
		}
		if traceID == "" {
			if t, _ := d["traceId"].(string); t != "" {
				trace = t // 같은 논리적 작업은 원격 홉까지 동일 traceId로 추적
			}
		}
	}
	m["delegation"] = map[string]any{
		"caller":  caller,
		"skill":   skill,
		"depth":   depth,
		"traceId": trace,
	}
	return m
}

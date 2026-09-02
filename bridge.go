// bridge.go — 중앙 A2A 엔드포인트(정책 게이트웨이). 릴레이가 아니라 검문소:
// 토큰 검증 → tenant 라우팅 → 위임 정책 판정 → Task 상태머신 운영.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type Bridge struct {
	mu         sync.Mutex
	key        []byte              // 토큰 서명 키 (AS 역할)
	internal   []byte              // 봇 포트 전용 내부 시크릿 (bypass 방어)
	tasks      map[string]*Task    // taskId → task
	byCaller   map[string][]string // token.Sub → taskIds (ListTasks용)
	nextID     int
	bots       map[string]*Bot
	baseURL    string // 브리지 자기 URL (카드 채움)
	exec       Executor
	fedInbound []byte                       // A2A_FEDERATION_INBOUND_TOKEN: 인바운드 federation Bearer (비면 폐쇄)
	fedClient  func(base string) *A2AClient // owner_decision 원격 중계용 클라이언트 팩토리
}

func NewBridge(baseURL string, bots map[string]*Bot) *Bridge {
	return &Bridge{
		key:        newSigningKey(),
		internal:   newSigningKey(),
		tasks:      map[string]*Task{},
		byCaller:   map[string][]string{},
		bots:       bots,
		baseURL:    baseURL,
		exec:       SimExecutor{}, // 기본: 시뮬레이션(실 fleet 무접촉). SetExecutor로 실전 교체.
		fedInbound: []byte(os.Getenv("A2A_FEDERATION_INBOUND_TOKEN")),
	}
}

// SetExecutor — 스킬 실행 어댑터 교체(sim 기본 → hermes 실전). 호출은 기동 시 1회.
func (b *Bridge) SetExecutor(e Executor) { b.exec = e }

// SetFedClient — 크로스호스트 owner_decision 중계용 federation 클라이언트
// 팩토리 주입 (RemoteExecutor.clientFor와 동일 구성을 공유).
func (b *Bridge) SetFedClient(fn func(base string) *A2AClient) { b.fedClient = fn }

// ---- 토큰 발급/교환 (AS 역할) ----

// MintClientToken — client_credentials 시뮬레이션: 봇 루트 토큰.
func (b *Bridge) MintClientToken(botID string, skills []string) string {
	return mint(b.key, Claims{Sub: botID, Skills: skills, Iat: now(), TraceID: newTraceID()})
}

// ExchangeToken — RFC 8693풍 교환: 부모 토큰을 자식 위임 토큰으로.
func (b *Bridge) ExchangeToken(parentTok, childSub string, skills []string) (string, error) {
	parent, err := verify(b.key, parentTok)
	if err != nil {
		return "", fmt.Errorf("parent token invalid: %w", err)
	}
	tok, _, err := exchange(b.key, parent, childSub, skills)
	return tok, err
}

// ---- HTTP ----

func (b *Bridge) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/agent-card.json", b.serveCard)
	mux.HandleFunc("POST /token/exchange", b.serveExchange)
	mux.HandleFunc("POST /", b.serveRPC)
	return mux
}

// Card — 단일 엔드포인트 뒤 테넌트별 인터페이스 (스펙 §4.4.6 tenant).
func (b *Bridge) Card() AgentCard {
	ifaces := []AgentInterface{}
	for id := range b.bots {
		ifaces = append(ifaces, AgentInterface{Name: id, URL: b.baseURL + "/", Tenant: id})
	}
	return AgentCard{
		Name:               "hermes-bridge",
		Description:        "A2A delegation gateway for the Hermes bot fleet",
		Version:            "1.0.0",
		Capabilities:       Capabilities{StateTransitionHistory: true},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		AgentInterfaces:    ifaces,
		Skills:             cardSkills(),
		SecuritySchemes:    map[string]SecurityScheme{"bearer": {Type: "http", Scheme: "bearer"}},
		PreferredTransport: "JSONRPC",
	}
}

func cardSkills() []Skill {
	out := []Skill{}
	for _, s := range Catalog {
		out = append(out, Skill{ID: s.ID, Name: s.ID, Description: fmt.Sprintf("served by %v (class %d)", s.Owners, s.Class)})
	}
	return out
}

func (b *Bridge) serveCard(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, b.Card())
}

type exchangeReq struct {
	Token  string   `json:"token"`
	Child  string   `json:"child"`
	Skills []string `json:"skills"`
}

func (b *Bridge) serveExchange(w http.ResponseWriter, r *http.Request) {
	var req exchangeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, RPCResponse{Error: &RPCError{Code: CodeParseError, Message: "bad exchange request"}})
		return
	}
	tok, err := b.ExchangeToken(req.Token, req.Child, req.Skills)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": tok})
}

// serveRPC — A2A JSON-RPC 진입점.
func (b *Bridge) serveRPC(w http.ResponseWriter, r *http.Request) {
	// 1) 전송 계층 인증: Bearer 토큰 (변조 → 401)
	auth := r.Header.Get("Authorization")
	if len(auth) < 8 || auth[:7] != "Bearer " {
		writeJSON(w, http.StatusUnauthorized, RPCResponse{Error: &RPCError{Code: CodeInvalidRequest, Message: "missing bearer token"}})
		return
	}
	claims, err := verify(b.key, auth[7:])
	if err != nil {
		// 인바운드 federation: A2A_FEDERATION_INBOUND_TOKEN와 상수시간 일치하는
		// Bearer면 수락. 이 경우 claims는 아래에서 파라미터 기반으로 합성한다.
		if subtle.ConstantTimeCompare([]byte(auth[7:]), b.fedInbound) != 1 || len(b.fedInbound) == 0 {
			writeJSON(w, http.StatusUnauthorized, RPCResponse{Error: &RPCError{Code: CodeInvalidRequest, Message: "token rejected: " + err.Error()}})
			return
		}
		claims = nil // federation 마커 — 본문 파싱 후 federationClaims로 합성
	}

	// 2) 버전 협상: A2A-Version 헤더. (스펙: 빈 값은 0.3 해석 — 우리는 1.0만 구현하므로 불일치 → -32009)
	if v := r.Header.Get("A2A-Version"); v != "1.0" {
		writeJSON(w, http.StatusOK, RPCResponse{Error: &RPCError{Code: CodeVersionNotSup, Message: fmt.Sprintf("VersionNotSupportedError: want 1.0, got %q", v)}})
		return
	}

	// 3) JSON-RPC 디스패치
	var req RPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusOK, RPCResponse{Error: &RPCError{Code: CodeParseError, Message: "parse error"}})
		return
	}
	if claims == nil {
		claims = federationClaims(r.Header.Get("X-A2A-Caller"), req.Params)
	}
	var result any
	var rpcErr *RPCError
	switch req.Method {
	case "SendMessage":
		result, rpcErr = b.SendMessage(req, claims)
	case "GetTask":
		result, rpcErr = b.GetTask(req)
	case "ListTasks":
		result, rpcErr = b.ListTasks(req, claims)
	case "CancelTask":
		result, rpcErr = b.CancelTask(req)
	default:
		rpcErr = &RPCError{Code: CodeMethodNotFound, Message: "method not found: " + req.Method}
	}
	resp := RPCResponse{JSONRPC: "2.0", ID: req.ID}
	if rpcErr != nil {
		resp.Error = rpcErr
	} else {
		resp.Result = result
	}
	writeJSON(w, http.StatusOK, resp)
}

// federationClaims — 인바운드 federation 호출(FedToken Bearer)에 대한 합성 claims.
// Sub = "federation:"+X-A2A-Caller, Skills = 요청 스킬(피어가 보증한 위임),
// Act = 메시지 위임 metadata depth D에 federation 홉 1개를 더한 D+1개의 더미 액터.
// 이대로 CheckDelegation을 통과하므로 합성 depth가 MaxDelegationDepth를 넘으면
// (예: D=2 → depth 3 > 2) 일반 경로와 동일하게 DENY된다 — depth 홉 증가 방어.
func federationClaims(caller string, params json.RawMessage) *Claims {
	var p MessageSendParams
	json.Unmarshal(params, &p) // 파싱 실패 시 빈 값 → SendMessage에서 본 파밍 에러
	skill, _ := p.Metadata["skill"].(string)
	c := &Claims{Sub: "federation:" + caller, Skills: []string{skill}, Iat: now(), TraceID: newTraceID()}
	depth := 0
	if d, _ := p.Message.Metadata["delegation"].(map[string]any); d != nil {
		if n, ok := d["depth"].(float64); ok && n >= 0 {
			depth = int(n)
		}
		if t, _ := d["traceId"].(string); t != "" {
			c.TraceID = t
		}
	}
	for i := 0; i < depth+1; i++ {
		c.Act = append(c.Act, Actor{Sub: fmt.Sprintf("federation-hop-%d", i+1)})
	}
	return c
}

// ---- 메서드 구현 ----

// SendMessage — 핵심 흐름: 정책 판정 → Task 생성. input-required 후
// 소유주 결정(metadata.owner_decision)이 두 번째 SendMessage로 도착하면 승격/취소.
func (b *Bridge) SendMessage(req RPCRequest, claims *Claims) (any, *RPCError) {
	var p MessageSendParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: "bad SendMessage params"}
	}
	if _, ok := b.bots[p.Tenant]; !ok {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: "unknown tenant: " + p.Tenant}
	}

	// 이전 태스크에 대한 소유주 결정 (ask_owner 후속 호출)
	if taskID, _ := p.Metadata["taskId"].(string); taskID != "" {
		return b.applyOwnerDecision(taskID, p, claims)
	}

	skill, _ := p.Metadata["skill"].(string)
	d := CheckDelegation(claims, p.Tenant, skill)

	task := b.newTask(p.Tenant, claims, skill, d)
	switch d.Verdict {
	case "deny":
		task.Status = TaskStatus{State: StateRejected, Message: &Message{Role: "agent", Parts: []Part{TextPart("DENY — " + d.Reason)}}}
		b.appendHistory(task)
	case "ask_owner":
		task.Status = TaskStatus{State: StateInputRequired, Message: &Message{Role: "agent", Parts: []Part{TextPart("ask_owner — " + d.Reason + " · reply via SendMessage with metadata.taskId + owner_decision")}}}
		b.appendHistory(task)
	case "allow":
		task.Status = TaskStatus{State: StateWorking}
		b.appendHistory(task)
		traceID, _ := task.Metadata["delegation"].(map[string]any)["traceId"].(string)
		out, err := b.exec.Execute(p.Tenant, skill, p.Message, traceID)
		if err != nil {
			// 원격 실행기(RemoteExecutor) 계약: input-required는 로컬도 input-required로
			// (Question 전달 + 원격 태스크 연결 기록), 원격 거절은 그 사유와 함께 rejected로.
			var eir *ErrInputRequired
			var ert *ErrRemoteTerminal
			switch {
			case errors.As(err, &eir):
				task.Status = TaskStatus{State: StateInputRequired, Message: &Message{Role: "agent", Parts: []Part{TextPart(eir.Question)}}}
				task.Metadata["remoteTaskId"] = eir.TaskID
				task.Metadata["remoteBaseURL"] = eir.BaseURL
			case errors.As(err, &ert) && ert.State == StateRejected:
				task.Status = TaskStatus{State: StateRejected, Message: &Message{Role: "agent", Parts: []Part{TextPart("remote rejected — " + ert.Reason)}}}
			default:
				task.Status = TaskStatus{State: StateFailed, Message: &Message{Role: "agent", Parts: []Part{TextPart("executor error — " + err.Error())}}}
			}
		} else {
			task.Status = TaskStatus{State: StateCompleted, Message: &Message{Role: "agent", Parts: []Part{TextPart(out)}}}
			task.Artifacts = append(task.Artifacts, Artifact{Name: "result", Parts: []Part{TextPart(out)},
				Metadata: map[string]any{"skill": skill, "tenant": p.Tenant}})
		}
		b.appendHistory(task)
	}
	return task, nil
}

// applyOwnerDecision — input-required 상태에서 사람의 결정 적용. 태스크가 원격
// (크로스호스트 federation)에 있으면(remoteTaskId 기록됨) 결정을 원격 브리지로
// 중계하고, 로컬 승인 실행 중 원격이 다시 input-required면 연결을 갱신해
// 다음 결정을 기다린다(동의 체인의 크로스호스트 전파).
func (b *Bridge) applyOwnerDecision(taskID string, p MessageSendParams, claims *Claims) (any, *RPCError) {
	b.mu.Lock()
	task, ok := b.tasks[taskID]
	b.mu.Unlock()
	if !ok {
		return nil, &RPCError{Code: CodeTaskNotFound, Message: "task not found: " + taskID}
	}
	if task.Status.State != StateInputRequired {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: "task " + taskID + " is " + task.Status.State + ", not awaiting owner decision"}
	}
	tenant, _ := task.Metadata["tenant"].(string)
	remoteTaskID, _ := task.Metadata["remoteTaskId"].(string)
	decision, _ := p.Metadata["owner_decision"].(string)
	switch decision {
	case "approve":
		if remoteTaskID != "" { // 원격 태스크가 이미 존재 → 결정을 원격으로 중계
			return b.relayOwnerDecision(task, tenant, remoteTaskID, "approve", p)
		}
		skill, _ := task.Metadata["skill"].(string)
		traceID, _ := task.Metadata["delegation"].(map[string]any)["traceId"].(string)
		out, err := b.exec.Execute(tenant, skill, p.Message, traceID)
		if err != nil {
			var eir *ErrInputRequired
			if errors.As(err, &eir) {
				task.Status = TaskStatus{State: StateInputRequired, Message: &Message{Role: "agent", Parts: []Part{TextPart(eir.Question)}}}
				task.Metadata["remoteTaskId"] = eir.TaskID
				task.Metadata["remoteBaseURL"] = eir.BaseURL
				break
			}
			task.Status = TaskStatus{State: StateFailed, Message: &Message{Role: "agent", Parts: []Part{TextPart("owner-approved but executor failed — " + err.Error())}}}
			break
		}
		task.Status = TaskStatus{State: StateCompleted, Message: &Message{Role: "agent", Parts: []Part{TextPart("owner-approved · " + out)}}}
		task.Artifacts = append(task.Artifacts, Artifact{Name: "result", Parts: []Part{TextPart(out)}})
	default: // deny 포함 기타
		if remoteTaskID != "" { // 원격 거절도 원격 태스크 취소로 전파
			return b.relayOwnerDecision(task, tenant, remoteTaskID, "deny", p)
		}
		task.Status = TaskStatus{State: StateCanceled, Message: &Message{Role: "agent", Parts: []Part{TextPart("owner-denied · task canceled")}}}
	}
	b.appendHistory(task)
	return task, nil
}

// relayOwnerDecision — 소유주 결정(approve/deny)을 원격 브리지의 태스크로
// SendMessage metadata{taskId: 원격ID, owner_decision} 중계하고 결과 상태를
// 로컬 태스크에 매핑한다.
func (b *Bridge) relayOwnerDecision(task *Task, tenant, remoteTaskID, decision string, p MessageSendParams) (any, *RPCError) {
	if b.fedClient == nil {
		task.Status = TaskStatus{State: StateFailed, Message: &Message{Role: "agent", Parts: []Part{TextPart("federation relay unavailable — SetFedClient not wired")}}}
		b.appendHistory(task)
		return task, nil
	}
	base, _ := task.Metadata["remoteBaseURL"].(string)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rt, rpcErr, err := b.fedClient(base).SendMessage(ctx, MessageSendParams{
		Tenant:   tenant,
		Message:  p.Message,
		Metadata: map[string]any{"taskId": remoteTaskID, "owner_decision": decision},
	})
	switch {
	case err != nil:
		task.Status = TaskStatus{State: StateFailed, Message: &Message{Role: "agent", Parts: []Part{TextPart("federation relay error — " + err.Error())}}}
	case rpcErr != nil:
		task.Status = TaskStatus{State: StateFailed, Message: &Message{Role: "agent", Parts: []Part{TextPart("federation relay rejected — " + rpcErr.Message)}}}
	case rt != nil && rt.Status.State == StateCompleted:
		out := taskText(rt)
		task.Status = TaskStatus{State: StateCompleted, Message: &Message{Role: "agent", Parts: []Part{TextPart("owner-approved · " + out)}}}
		task.Artifacts = append(task.Artifacts, Artifact{Name: "result", Parts: []Part{TextPart(out)}})
	case rt != nil && rt.Status.State == StateCanceled:
		task.Status = TaskStatus{State: StateCanceled, Message: &Message{Role: "agent", Parts: []Part{TextPart("owner-denied · task canceled (remote)")}}}
	default:
		state, why := "unknown", ""
		if rt != nil {
			state = rt.Status.State
			if rt.Status.Message != nil {
				why = textOf(*rt.Status.Message)
			}
		}
		task.Status = TaskStatus{State: StateFailed, Message: &Message{Role: "agent", Parts: []Part{TextPart("remote " + state + " — " + why)}}}
	}
	b.appendHistory(task)
	return task, nil
}

func (b *Bridge) GetTask(req RPCRequest) (any, *RPCError) {
	var p GetTaskParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: "bad GetTask params"}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	task, ok := b.tasks[p.TaskID]
	if !ok {
		return nil, &RPCError{Code: CodeTaskNotFound, Message: "task not found: " + p.TaskID}
	}
	return task, nil
}

func (b *Bridge) ListTasks(req RPCRequest, claims *Claims) (any, *RPCError) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := []*Task{}
	for _, id := range b.byCaller[claims.Sub] {
		out = append(out, b.tasks[id])
	}
	return out, nil
}

func (b *Bridge) CancelTask(req RPCRequest) (any, *RPCError) {
	var p CancelTaskParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: "bad CancelTask params"}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	task, ok := b.tasks[p.TaskID]
	if !ok {
		return nil, &RPCError{Code: CodeTaskNotFound, Message: "task not found: " + p.TaskID}
	}
	if task.Status.State == StateCompleted || task.Status.State == StateCanceled {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: "task already terminal: " + task.Status.State}
	}
	task.Status = TaskStatus{State: StateCanceled}
	b.appendHistory(task)
	return task, nil
}

// ---- task helpers ----

func (b *Bridge) newTask(tenant string, claims *Claims, skill string, d PolicyDecision) *Task {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	id := fmt.Sprintf("task-%03d", b.nextID)
	t := &Task{
		ID:        id,
		ContextID: fmt.Sprintf("ctx-%s", claims.TraceID),
		Status:    TaskStatus{State: StateSubmitted},
		Metadata: map[string]any{
			"delegation": map[string]any{
				"traceId": claims.TraceID,
				"caller":  claims.Sub,
				"chain":   chainString(claims),
				"depth":   len(claims.Act),
				"skill":   skill,
				"verdict": d.Verdict,
			},
			"skill":  skill,
			"tenant": tenant,
		},
	}
	b.tasks[id] = t
	b.byCaller[claims.Sub] = append(b.byCaller[claims.Sub], id)
	b.appendHistory(t)
	return t
}

func (b *Bridge) appendHistory(t *Task) {
	t.History = append(t.History, t.Status)
}

// InternalSecret — 봇 포트 검증용 (상수시간 비교).
func (b *Bridge) InternalSecret() []byte { return b.internal }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

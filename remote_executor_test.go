// remote_executor_test.go — 크로스호스트 federation 검증. "서로 다른 위치(호스트)"를
// 같은 프로세스의 포트가 다른 httptest 브리지 2개로 재현한다:
//
//	브리지 A(클라이언트 측, jena가 HMAC 토큰으로 호출) ──RemoteExecutor/FedToken──▶
//	브리지 B(원격 측, lumi 소유, federation 인바운드 수락)
//
// 테스트의 모든 외부 호출(jena·소유주·raw federation 프로브)은 아래 내장 http
// 헬퍼가 직접 JSON-RPC를 쏜다. RemoteExecutor의 원격 호출만 NewClient 주입
// A2AClient(R1)을 경유하고, 그 transport에서 나가는 SendMessage를 녹화해
// 위임 metadata를 검증한다.
//
// 시나리오:
//
//	① A로 jena가 보낸 요청이 B의 [lumi] 실행 결과를 A의 artifact로
//	② sensitive(dm.read): 원격 ask_owner → A에서 승인 → 결정이 B로 중계 → completed
//	③ 원격 거절(unknown tenant)이 A 태스크 rejected로 전파
//	④ depth 방어: delegation depth 2 → 합성 depth 3 > 2 → 거절 (depth 1은 통과)
//	⑤ FedToken 불일치 → 401
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fedRPC — X-A2A-Caller 헤더를 추가로 실을 수 있는 테스트 내장 JSON-RPC 호출
// (bridge_test.go의 rpc와 동일 응답 계약).
func fedRPC(t *testing.T, base, method, token, caller string, params any) (int, *Task, *RPCResponse) {
	t.Helper()
	body := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", base+"/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if caller != "" {
		req.Header.Set("X-A2A-Caller", caller)
	}
	req.Header.Set("A2A-Version", "1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fedRPC %s: %v", method, err)
	}
	defer resp.Body.Close()
	var r RPCResponse
	json.NewDecoder(resp.Body).Decode(&r)
	var task Task
	if r.Result != nil {
		b, _ := json.Marshal(r.Result)
		json.Unmarshal(b, &task)
	}
	return resp.StatusCode, &task, &r
}

// sendRecorder — A2AClient.HTTP 주입용 녹화 transport: A→B로 나가는 SendMessage
// params를 캡처한다 (위임 metadata 검증). 요청은 그대로 통과시킨다.
type sendRecorder struct {
	mu    sync.Mutex
	sends []MessageSendParams
}

func (sr *sendRecorder) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Body != nil {
		raw, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var req struct {
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(raw, &req) == nil && req.Method == "SendMessage" {
			var p MessageSendParams
			if json.Unmarshal(req.Params, &p) == nil {
				sr.mu.Lock()
				sr.sends = append(sr.sends, p)
				sr.mu.Unlock()
			}
		}
	}
	return http.DefaultTransport.RoundTrip(r)
}

func (sr *sendRecorder) captured() []MessageSendParams {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return append([]MessageSendParams{}, sr.sends...)
}

// fedEnv — 크로스호스트 테스트 환경: A(클라이언트 브리지) + B(원격 브리지).
type fedEnv struct {
	A, B   *httptest.Server
	jena   string // A가 발급한 jena 루트 토큰
	fedTok string // A→B federation Bearer (= B의 인바운드 토큰)
	sent   *sendRecorder
}

// fedSetup — B는 lumi(+jena) 소유 + federation 인바운드 ON(NewBridge가 env를
// 읽는 순간만 노출), A는 RemoteExecutor로 lumi/minami을 B로 라우팅. minami는
// B에 없는 테넌트로, 원격 거절 전파(③)를 유도하는 라우트다.
func fedSetup(t *testing.T) *fedEnv {
	t.Helper()
	fedTok := "fed-token-crosshost-test"
	os.Setenv("A2A_FEDERATION_INBOUND_TOKEN", fedTok)
	t.Cleanup(func() { os.Unsetenv("A2A_FEDERATION_INBOUND_TOKEN") })

	botsB := map[string]*Bot{"lumi": NewBot("lumi"), "jena": NewBot("jena")}
	brB := NewBridge("", botsB)
	for _, b := range botsB {
		b.SetBridgeSecret(string(brB.InternalSecret()))
	}
	srvB := httptest.NewServer(brB.Handler())
	t.Cleanup(srvB.Close)
	os.Unsetenv("A2A_FEDERATION_INBOUND_TOKEN") // A는 federation 인바운드 없이 생성

	rec := &sendRecorder{}
	re := &RemoteExecutor{
		Routes:   map[string]string{"lumi": srvB.URL, "minami": srvB.URL},
		FedToken: fedTok,
		Caller:   "bridgeA",
		Poll:     2 * time.Millisecond,
		Timeout:  2 * time.Second,
		NewClient: func(base string) *A2AClient {
			return &A2AClient{BaseURL: base, Token: fedTok,
				HTTP: &http.Client{Transport: &callerTransport{caller: "bridgeA", next: rec}}}
		},
	}
	botsA := map[string]*Bot{"jena": NewBot("jena"), "lumi": NewBot("lumi"), "minami": NewBot("minami")}
	brA := NewBridge("", botsA)
	brA.SetExecutor(re)
	brA.SetFedClient(re.clientFor)
	for _, b := range botsA {
		b.SetBridgeSecret(string(brA.InternalSecret()))
	}
	srvA := httptest.NewServer(brA.Handler())
	t.Cleanup(srvA.Close)
	return &fedEnv{
		A: srvA, B: srvB, fedTok: fedTok, sent: rec,
		jena: brA.MintClientToken("jena", []string{"logs.summarize", "dm.read", "profile.digest"}),
	}
}

func statusText(task *Task) string {
	if task.Status.Message == nil || len(task.Status.Message.Parts) == 0 {
		return ""
	}
	return task.Status.Message.Parts[0].Text
}

// ① 정상 크로스호스트 실행: jena@A → B의 [lumi] 결과가 A의 artifact로.
func TestFedCrossHostArtifact(t *testing.T) {
	e := fedSetup(t)
	_, task, r := rpc(t, e.A.URL, "SendMessage", e.jena, sendParams("lumi", "logs.summarize"))
	if r.Error != nil {
		t.Fatalf("rpc error: %+v", r.Error)
	}
	if task.Status.State != StateCompleted {
		t.Fatalf("A task state=%s want completed · msg=%s", task.Status.State, statusText(task))
	}
	if len(task.Artifacts) == 0 || !strings.Contains(task.Artifacts[0].Parts[0].Text, "[lumi]") {
		t.Fatalf("A artifact must carry B's [lumi] result: %+v", task.Artifacts)
	}

	// 나가는 위임 추적 metadata: {caller, skill, depth}
	sends := e.sent.captured()
	if len(sends) == 0 {
		t.Fatal("no outbound SendMessage captured from A to B")
	}
	d, _ := sends[0].Message.Metadata["delegation"].(map[string]any)
	if d == nil || d["caller"] != "bridgeA" || d["skill"] != "logs.summarize" || d["depth"] != float64(0) {
		t.Fatalf("outgoing delegation metadata wrong: %+v", d)
	}
	if sends[0].Tenant != "lumi" || sends[0].Metadata["skill"] != "logs.summarize" {
		t.Fatalf("outbound params wrong: tenant=%q meta=%+v", sends[0].Tenant, sends[0].Metadata)
	}

	// B에 실제로 federation 신원(federation:bridgeA)의 태스크가 completed로 생겼는지
	_, _, rb := fedRPC(t, e.B.URL, "ListTasks", e.fedTok, "bridgeA", ListTasksParams{Tenant: "lumi"})
	var bt []Task
	bb, _ := json.Marshal(rb.Result)
	json.Unmarshal(bb, &bt)
	if len(bt) != 1 || bt[0].Status.State != StateCompleted {
		t.Fatalf("B should hold 1 completed federation task, got %+v", bt)
	}
	dm, _ := bt[0].Metadata["delegation"].(map[string]any)
	if dm == nil || dm["caller"] != "federation:bridgeA" {
		t.Fatalf("B task delegation caller wrong: %+v", dm)
	}
}

// ② sensitive 스킬의 크로스호스트 동의 전파: A ask_owner → 승인 → B ask_owner →
// A 재승인 → 결정이 B로 중계 → 양쪽 모두 completed.
func TestFedOwnerApprovalPropagates(t *testing.T) {
	e := fedSetup(t)

	// 1) A에서 dm.read → 로컬 ask_owner
	_, task, r := rpc(t, e.A.URL, "SendMessage", e.jena, sendParams("lumi", "dm.read"))
	if r.Error != nil || task.Status.State != StateInputRequired {
		t.Fatalf("want local ask_owner/input-required: state=%s err=%+v", task.Status.State, r.Error)
	}

	// 2) 첫 승인 → 원격 실행 → B도 ask_owner → A 태스크는 원격 연결과 함께 재입장
	approve := func(id string) MessageSendParams {
		return MessageSendParams{
			Tenant:   "lumi",
			Message:  Message{Role: "user", Parts: []Part{TextPart("approved")}},
			Metadata: map[string]any{"taskId": id, "owner_decision": "approve"},
		}
	}
	_, t2, ra := rpc(t, e.A.URL, "SendMessage", e.jena, approve(task.ID))
	if ra.Error != nil || t2.Status.State != StateInputRequired {
		t.Fatalf("want remote ask_owner surfaced at A: state=%s err=%+v msg=%s", t2.Status.State, ra.Error, statusText(t2))
	}
	if !strings.Contains(statusText(t2), "dm.read") {
		t.Fatalf("question should carry remote ask_owner reason: %q", statusText(t2))
	}
	rtID, _ := t2.Metadata["remoteTaskId"].(string)
	if rtID == "" {
		t.Fatal("task.Metadata.remoteTaskId missing after remote ask_owner")
	}

	// 3) 두 번째 승인 → A가 B로 owner_decision 중계 → B completed → A completed
	_, done, ra2 := rpc(t, e.A.URL, "SendMessage", e.jena, approve(t2.ID))
	if ra2.Error != nil || done.Status.State != StateCompleted {
		t.Fatalf("relay approve failed: state=%s err=%+v msg=%s", done.Status.State, ra2.Error, statusText(done))
	}
	if len(done.Artifacts) == 0 || !strings.Contains(done.Artifacts[0].Parts[0].Text, "[lumi] DM digest") {
		t.Fatalf("completed artifact must carry B's dm.read result: %+v", done.Artifacts)
	}

	// 4) 크로스호스트 증거: B의 원격 태스크 자체도 completed
	_, bt, rb := fedRPC(t, e.B.URL, "GetTask", e.fedTok, "bridgeA", GetTaskParams{TaskID: rtID, Tenant: "lumi"})
	if rb.Error != nil || bt.Status.State != StateCompleted {
		t.Fatalf("remote task should be completed after relay: state=%s err=%+v", bt.Status.State, rb.Error)
	}
}

// ③ 원격 거절 전파: A는 minami 라우트를 갖지만 B에는 minami 테넌트가 없다.
func TestFedRemoteRejectionPropagates(t *testing.T) {
	e := fedSetup(t)
	_, task, r := rpc(t, e.A.URL, "SendMessage", e.jena, sendParams("minami", "profile.digest"))
	if r.Error != nil {
		t.Fatalf("rpc error: %+v", r.Error)
	}
	if task.Status.State != StateRejected {
		t.Fatalf("A task want rejected (remote rejection), got %s · msg=%s", task.Status.State, statusText(task))
	}
	if !strings.Contains(statusText(task), "unknown tenant: minami") {
		t.Fatalf("rejection message must carry remote reason: %q", statusText(task))
	}
}

// ④ federation depth 방어: delegation depth 2 → 합성 체인 3 > MaxDelegationDepth 2
// → 거절. 대조로 depth 1(합성 2)은 통과.
func TestFedDepthDefense(t *testing.T) {
	e := fedSetup(t)
	msg := func(depth int) MessageSendParams {
		return MessageSendParams{
			Tenant: "lumi",
			Message: Message{Role: "agent", Parts: []Part{TextPart("chained")}, Metadata: map[string]any{
				"delegation": map[string]any{"caller": "bridgeX", "skill": "logs.summarize", "depth": depth},
			}},
			Metadata: map[string]any{"skill": "logs.summarize"},
		}
	}
	_, deep, r := fedRPC(t, e.B.URL, "SendMessage", e.fedTok, "bridgeX", msg(2))
	if r.Error != nil {
		t.Fatalf("rpc error: %+v", r.Error)
	}
	if deep.Status.State != StateRejected || !strings.Contains(statusText(deep), "depth 3 exceeds max 2") {
		t.Fatalf("depth-2 inbound must synthesize depth 3 and reject: state=%s msg=%s", deep.Status.State, statusText(deep))
	}
	_, shallow, r2 := fedRPC(t, e.B.URL, "SendMessage", e.fedTok, "bridgeX", msg(1))
	if r2.Error != nil || shallow.Status.State != StateCompleted {
		t.Fatalf("depth-1 inbound should pass (synth depth 2): state=%s err=%+v msg=%s", shallow.Status.State, r2.Error, statusText(shallow))
	}
}

// ⑤ FedToken 불일치 → 401 (federation 인바운드는 HMAC 경로로 폴스루).
func TestFedBadTokenRejected(t *testing.T) {
	e := fedSetup(t)
	code, _, _ := fedRPC(t, e.B.URL, "SendMessage", "wrong-fed-token", "bridgeA", sendParams("lumi", "logs.summarize"))
	if code != http.StatusUnauthorized {
		t.Fatalf("mismatched FedToken want 401, got %d", code)
	}
}

// ⑥ 원격 working 상태 폴링: SendMessage가 working을 반환하면 WaitTerminal로
// 종료 상태까지 폴링한 뒤 재매핑한다. 스크립티드 원격 엔드포인트(working 1회 →
// completed)로 실제 A2AClient HTTP 폴링 경로를 지난다. 라우트 누락 에러도 함께.
func TestFedPollsRemoteWorkingState(t *testing.T) {
	var polls int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req RPCRequest
		json.NewDecoder(r.Body).Decode(&req)
		task := func(state string) *Task {
			tk := &Task{ID: "task-remote-1", ContextID: "ctx", Status: TaskStatus{State: state}}
			if state == StateCompleted {
				tk.Status.Message = &Message{Role: "agent", Parts: []Part{TextPart("[lumi] async result")}}
				tk.Artifacts = append(tk.Artifacts, Artifact{Name: "result", Parts: []Part{TextPart("[lumi] async result")}})
			}
			return tk
		}
		switch req.Method {
		case "SendMessage":
			writeJSON(w, http.StatusOK, RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: task(StateWorking)})
		case "GetTask":
			atomic.AddInt32(&polls, 1)
			state := StateWorking
			if atomic.LoadInt32(&polls) >= 2 {
				state = StateCompleted
			}
			writeJSON(w, http.StatusOK, RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: task(state)})
		default:
			writeJSON(w, http.StatusOK, RPCResponse{JSONRPC: "2.0", ID: req.ID,
				Error: &RPCError{Code: CodeMethodNotFound, Message: "unexpected " + req.Method}})
		}
	}))
	t.Cleanup(remote.Close)

	re := &RemoteExecutor{
		Routes:  map[string]string{"lumi": remote.URL},
		Poll:    2 * time.Millisecond,
		Timeout: 2 * time.Second,
		NewClient: func(base string) *A2AClient {
			return &A2AClient{BaseURL: base}
		},
	}
	out, err := re.Execute("lumi", "logs.summarize", Message{Parts: []Part{TextPart("async")}}, "trace-test")
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out != "[lumi] async result" {
		t.Fatalf("polled result wrong: %q", out)
	}
	if n := atomic.LoadInt32(&polls); n < 2 {
		t.Fatalf("expected polling until terminal, polls=%d", n)
	}

	// 라우트 누락 프로파일은 즉시 에러
	if _, err := re.Execute("yena", "weather.report", Message{}, ""); err == nil || !strings.Contains(err.Error(), "no route") {
		t.Fatalf("missing route should error: %v", err)
	}
}

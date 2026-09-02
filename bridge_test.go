// bridge_test.go — 5개 핵심 시나리오 + 보안 변형 2종을 실제 HTTP로 검증.
// httptest 사용(포트 고정 없음), 전 시나리오 같은 bridge 인스턴스 공유.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type env struct {
	bridge  *httptest.Server
	lumiBot *httptest.Server
	jena    string // 루트 토큰
	minami  string
}

func setup(t *testing.T) *env {
	t.Helper()
	bots := map[string]*Bot{
		"jena": NewBot("jena"), "lumi": NewBot("lumi"),
		"yena": NewBot("yena"), "minami": NewBot("minami"),
	}
	bs := httptest.NewServer(bots["lumi"].Handler())
	t.Cleanup(bs.Close)
	br := NewBridge("", bots)
	for _, b := range bots {
		b.SetBridgeSecret(string(br.InternalSecret()))
	}
	srv := httptest.NewServer(br.Handler())
	t.Cleanup(srv.Close)
	return &env{
		bridge:  srv,
		lumiBot: bs,
		jena:    br.MintClientToken("jena", []string{"logs.summarize", "dm.read"}),
		minami:  br.MintClientToken("minami", []string{"logs.summarize"}),
	}
}

// rpc — 인증+버전 헤더를 갖춘 JSON-RPC 호출. 결과 Task와 RPCError를 함께 반환.
func rpc(t *testing.T, base, method, token string, params any) (int, *Task, *RPCResponse) {
	t.Helper()
	body := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", base+"/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("A2A-Version", "1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("rpc %s: %v", method, err)
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

func sendParams(tenant, skill string) MessageSendParams {
	return MessageSendParams{
		Tenant:   tenant,
		Message:  Message{Role: "agent", Parts: []Part{TextPart("request")}},
		Metadata: map[string]any{"skill": skill},
	}
}

// ── 카드/테넌트 ──

func TestBridgeCardExposesTenants(t *testing.T) {
	e := setup(t)
	resp, err := http.Get(e.bridge.URL + "/.well-known/agent-card.json")
	if err != nil {
		t.Fatal(err)
	}
	var card AgentCard
	json.NewDecoder(resp.Body).Decode(&card)
	want := map[string]bool{"jena": false, "lumi": false, "yena": false, "minami": false}
	for _, i := range card.AgentInterfaces {
		if _, ok := want[i.Tenant]; ok {
			want[i.Tenant] = true
		}
	}
	for tenant, found := range want {
		if !found {
			t.Errorf("card missing tenant %q", tenant)
		}
	}
	if _, ok := card.SecuritySchemes["bearer"]; !ok {
		t.Error("card missing bearer securityScheme")
	}
}

// ── 시나리오 1: 정상 위임 체인 ──

func TestScenario1NormalChain(t *testing.T) {
	e := setup(t)
	code, task, r := rpc(t, e.bridge.URL, "SendMessage", e.jena, sendParams("lumi", "logs.summarize"))
	if code != 200 || r.Error != nil {
		t.Fatalf("code=%d err=%+v", code, r.Error)
	}
	if task.Status.State != StateCompleted {
		t.Fatalf("state=%s want completed · msg=%v", task.Status.State, task.Status.Message)
	}
	if len(task.Artifacts) == 0 || !strings.Contains(task.Artifacts[0].Parts[0].Text, "[lumi]") {
		t.Fatalf("artifact missing: %+v", task.Artifacts)
	}
	d, _ := task.Metadata["delegation"].(map[string]any)
	if d == nil || d["traceId"] == "" || d["caller"] != "jena" || d["depth"] != float64(0) {
		t.Fatalf("delegation metadata wrong: %+v", d)
	}

	// GetTask 재조회 동일성
	_, again, r2 := rpc(t, e.bridge.URL, "GetTask", e.jena, GetTaskParams{TaskID: task.ID, Tenant: "lumi"})
	if r2.Error != nil || again.ID != task.ID || again.Status.State != StateCompleted {
		t.Fatalf("GetTask mismatch: %+v err=%+v", again, r2.Error)
	}
	// ListTasks: jena 이름으로 1건
	_, list, r3 := rpc(t, e.bridge.URL, "ListTasks", e.jena, ListTasksParams{Tenant: "lumi"})
	if r3.Error != nil || list == nil || len(list.Artifacts) < 0 { // list는 Task 배열 → 아래에서 원본 확인
		_ = list
	}
	var raw []Task
	bb, _ := json.Marshal(r3.Result)
	json.Unmarshal(bb, &raw)
	if len(raw) != 1 || raw[0].ID != task.ID {
		t.Fatalf("ListTasks want 1 task %s, got %+v", task.ID, raw)
	}
}

// ── 시나리오 2: 권한 밖/elevated 스킬 → DENY ──

func TestScenario2SkillDenied(t *testing.T) {
	e := setup(t)

	// 2-a: elevated 스킬은 소유주 직접 실행만 — 위임 불가
	_, task, r := rpc(t, e.bridge.URL, "SendMessage", e.jena, sendParams("yena", "deploy.reset"))
	if r.Error != nil {
		t.Fatalf("unexpected rpc error: %+v", r.Error)
	}
	if task.Status.State != StateRejected || !strings.Contains(task.Status.Message.Parts[0].Text, "elevated") {
		t.Fatalf("elevated deny failed: state=%s msg=%+v", task.Status.State, task.Status.Message)
	}

	// 2-b: normal 스킬이어도 grant에 없으면 거부 (권한 축소 원칙)
	_, task2, r2 := rpc(t, e.bridge.URL, "SendMessage", e.jena, sendParams("yena", "weather.report"))
	if r2.Error != nil || task2.Status.State != StateRejected || !strings.Contains(task2.Status.Message.Parts[0].Text, "not in delegation grant") {
		t.Fatalf("grant deny failed: state=%s err=%+v msg=%+v", task2.Status.State, r2.Error, task2.Status.Message)
	}
}

// ── 시나리오 3: 민감 스킬 → ask_owner → 승인/거절 ──

func TestScenario3AskOwner(t *testing.T) {
	e := setup(t)
	_, task, r := rpc(t, e.bridge.URL, "SendMessage", e.jena, sendParams("lumi", "dm.read"))
	if r.Error != nil {
		t.Fatalf("rpc error: %+v", r.Error)
	}
	if task.Status.State != StateInputRequired || !strings.Contains(task.Status.Message.Parts[0].Text, "ask_owner") {
		t.Fatalf("want input-required/ask_owner, got %s / %+v", task.Status.State, task.Status.Message)
	}

	// 3-a: 소유주 승인 → completed
	approve := MessageSendParams{
		Tenant:   "lumi",
		Message:  Message{Role: "user", Parts: []Part{TextPart("approved")}},
		Metadata: map[string]any{"taskId": task.ID, "owner_decision": "approve"},
	}
	_, done, ra := rpc(t, e.bridge.URL, "SendMessage", e.jena, approve)
	if ra.Error != nil || done.Status.State != StateCompleted || !strings.Contains(done.Status.Message.Parts[0].Text, "owner-approved") {
		t.Fatalf("approve flow failed: state=%s err=%+v", done.Status.State, ra.Error)
	}

	// 3-b: 다른 태스크는 소유주 거절 → canceled
	_, t2, _ := rpc(t, e.bridge.URL, "SendMessage", e.jena, sendParams("lumi", "dm.read"))
	deny := MessageSendParams{
		Tenant: "lumi", Message: Message{Role: "user", Parts: []Part{TextPart("no")}},
		Metadata: map[string]any{"taskId": t2.ID, "owner_decision": "deny"},
	}
	_, canceled, rd := rpc(t, e.bridge.URL, "SendMessage", e.jena, deny)
	if rd.Error != nil || canceled.Status.State != StateCanceled || !strings.Contains(canceled.Status.Message.Parts[0].Text, "owner-denied") {
		t.Fatalf("deny flow failed: state=%s err=%+v", canceled.Status.State, rd.Error)
	}
}

// ── 시나리오 4: bridge 우회 직접 호출 → 403 ──

func TestScenario4BypassRejected(t *testing.T) {
	e := setup(t)
	// 4-a: bridge 발급 정상 토큰으로 직접 호출(시크릿 헤더 없음) → 403
	body := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "SendMessage",
		"params": sendParams("lumi", "logs.summarize")}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", e.lumiBot.URL+"/", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+e.jena)
	req.Header.Set("A2A-Version", "1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(m["error"].(string), "bridge-only") {
		t.Fatalf("bypass(no header) want 403/bridge-only, got %d %v", resp.StatusCode, m)
	}

	// 4-b: 시크릿 헤더를 흉내내도 값이 틀리면 → 403
	req2, _ := http.NewRequest("POST", e.lumiBot.URL+"/", bytes.NewReader(raw))
	req2.Header.Set("X-Hermes-Bridge", "forged-secret")
	resp2, _ := http.DefaultClient.Do(req2)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("bypass(forged secret) want 403, got %d", resp2.StatusCode)
	}
}

// ── 시나리오 5: 위임 depth 초과 → DENY ──

func TestScenario5DepthExceeded(t *testing.T) {
	e := setup(t)
	// minami →(교환) jena →(교환) lumi →(교환) yena : act 체인 3홉
	tok := e.minami
	for _, next := range []string{"jena", "lumi", "yena"} {
		var err error
		tok, err = exchangeHTTP(t, e.bridge.URL, tok, next, []string{"logs.summarize"})
		if err != nil {
			t.Fatalf("exchange to %s: %v", next, err)
		}
	}
	_, task, r := rpc(t, e.bridge.URL, "SendMessage", tok, sendParams("lumi", "logs.summarize"))
	if r.Error != nil {
		t.Fatalf("rpc error: %+v", r.Error)
	}
	if task.Status.State != StateRejected || !strings.Contains(task.Status.Message.Parts[0].Text, "depth 3 exceeds max 2") {
		t.Fatalf("depth deny failed: state=%s msg=%+v", task.Status.State, task.Status.Message)
	}
	// 대조: 2홉 체인(minami→jena→lumi 호출)은 통과해야 정상
	tok2, err := exchangeHTTP(t, e.bridge.URL, e.minami, "jena", []string{"logs.summarize"})
	if err != nil {
		t.Fatal(err)
	}
	tok3, err := exchangeHTTP(t, e.bridge.URL, tok2, "lumi", []string{"logs.summarize"})
	if err != nil {
		t.Fatal(err)
	}
	_, ok, r3 := rpc(t, e.bridge.URL, "SendMessage", tok3, sendParams("lumi", "logs.summarize"))
	if r3.Error != nil || ok.Status.State != StateCompleted {
		t.Fatalf("2-hop chain should pass: state=%s err=%+v", ok.Status.State, r3.Error)
	}
}

// ── 보안 변형: 변조 토큰 / 버전 헤더 ──

func TestTamperedTokenRejected(t *testing.T) {
	e := setup(t)
	parts := strings.Split(e.jena, ".")
	forged := fmt.Sprintf("%s.%s.%s", parts[0], strings.Repeat("e", len(parts[1])), parts[2])
	code, _, _ := rpc(t, e.bridge.URL, "SendMessage", forged, sendParams("lumi", "logs.summarize"))
	if code != http.StatusUnauthorized {
		t.Fatalf("tampered token want 401, got %d", code)
	}
}

func TestVersionHeaderEnforced(t *testing.T) {
	e := setup(t)
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "SendMessage", "params": sendParams("lumi", "logs.summarize")})
	req, _ := http.NewRequest("POST", e.bridge.URL+"/", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+e.jena)
	req.Header.Set("A2A-Version", "0.3")
	resp, _ := http.DefaultClient.Do(req)
	var r RPCResponse
	json.NewDecoder(resp.Body).Decode(&r)
	if r.Error == nil || r.Error.Code != CodeVersionNotSup {
		t.Fatalf("want -32009, got %+v", r.Error)
	}
}

// exchangeHTTP — /token/exchange 호출.
func exchangeHTTP(t *testing.T, base, parent, child string, skills []string) (string, error) {
	t.Helper()
	raw, _ := json.Marshal(exchangeReq{Token: parent, Child: child, Skills: skills})
	resp, err := http.Post(base+"/token/exchange", "application/json", bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Token string `json:"token"`
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if out.Error != "" {
		return "", fmt.Errorf("%s", out.Error)
	}
	return out.Token, nil
}

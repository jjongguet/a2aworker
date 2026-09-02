// a2aclient_test.go — A2AClient(원격 호출 측) 검증. 같은 프로세스에서 다른 포트로
// '원격' bridge(httptest + NewBridge + SimExecutor)를 띄워, 서로 다른 위치의
// 에이전트 연합(federation)을 모형화한다. 실행: go test -run TestA2AClient -v .
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// a2aRemote — '원격 위치'의 A2A 엔드포인트 1곳(bridge 1개 + 4봇)과
// 호출자(jena)의 루트 토큰. 클라이언트 입장에서는 그냥 남의 BaseURL.
type a2aRemote struct {
	srv *httptest.Server
	br  *Bridge
	tok string
}

func startA2ARemote(t *testing.T) *a2aRemote {
	t.Helper()
	bots := map[string]*Bot{"jena": NewBot("jena"), "lumi": NewBot("lumi"), "yena": NewBot("yena"), "minami": NewBot("minami")}
	br := NewBridge("", bots)
	srv := httptest.NewServer(br.Handler())
	t.Cleanup(srv.Close)
	return &a2aRemote{srv: srv, br: br, tok: br.MintClientToken("jena", []string{"logs.summarize", "dm.read"})}
}

func (e *a2aRemote) client() *A2AClient {
	return &A2AClient{BaseURL: e.srv.URL, Token: e.tok}
}

func sumParams() MessageSendParams {
	return MessageSendParams{
		Tenant:   "lumi",
		Message:  Message{Role: "agent", Parts: []Part{TextPart("summarize yesterday's fleet logs")}},
		Metadata: map[string]any{"skill": "logs.summarize"},
	}
}

// ① FetchCard — 원격 카드 조회로 4 tenant 인터페이스(lumi/yena/jena/minami) 반환.
func TestA2AClientFetchCardTenants(t *testing.T) {
	e := startA2ARemote(t)
	card, err := e.client().FetchCard(context.Background())
	if err != nil {
		t.Fatalf("FetchCard: %v", err)
	}
	if card.Name != "hermes-bridge" {
		t.Errorf("card name = %q, want hermes-bridge", card.Name)
	}
	got := map[string]bool{}
	for _, i := range card.AgentInterfaces {
		got[i.Tenant] = true
	}
	for _, want := range []string{"lumi", "yena", "jena", "minami"} {
		if !got[want] {
			t.Errorf("card missing tenant interface %q (got %v)", want, got)
		}
	}
	if _, ok := card.SecuritySchemes["bearer"]; !ok {
		t.Error("card missing bearer securityScheme")
	}
}

// ② SendMessage — 원격 정책 통과(logs.summarize 위임) → completed Task + artifact.
func TestA2AClientSendMessageCompleted(t *testing.T) {
	e := startA2ARemote(t)
	task, rpcErr, err := e.client().SendMessage(context.Background(), sumParams())
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if rpcErr != nil {
		t.Fatalf("rpc error: %+v", rpcErr)
	}
	if task.Status.State != StateCompleted {
		t.Fatalf("state = %s, want completed (msg=%+v)", task.Status.State, task.Status.Message)
	}
	if len(task.Artifacts) == 0 || !strings.Contains(task.Artifacts[0].Parts[0].Text, "[lumi]") {
		t.Fatalf("artifact missing lumi result: %+v", task.Artifacts)
	}
	d, _ := task.Metadata["delegation"].(map[string]any)
	if d == nil || d["caller"] != "jena" {
		t.Fatalf("delegation metadata caller want jena: %+v", d)
	}
}

// ③ GetTask — 존재하지 않는 taskId → (nil, RPCError -32002, nil).
func TestA2AClientGetTaskNotFound(t *testing.T) {
	e := startA2ARemote(t)
	task, rpcErr, err := e.client().GetTask(context.Background(), "task-999")
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if task != nil {
		t.Fatalf("task must be nil on RPCError, got %+v", task)
	}
	if rpcErr == nil || rpcErr.Code != CodeTaskNotFound {
		t.Fatalf("want RPCError %d, got %+v", CodeTaskNotFound, rpcErr)
	}
	if !strings.Contains(rpcErr.Message, "task-999") {
		t.Errorf("error message should name the missing task: %q", rpcErr.Message)
	}
}

// ④ WaitTerminal — completed 도달 시 반환 + input-required는 터미널이 아니므로
// 폴링이 지속되고 caller의 ctx 만료로 중단된다(DeadlineExceeded).
func TestA2AClientWaitTerminal(t *testing.T) {
	e := startA2ARemote(t)
	c := e.client()

	// 4-a: completed 태스크 → 폴링 첫 회차에서 즉시 반환
	sent, rpcErr, err := c.SendMessage(context.Background(), sumParams())
	if err != nil || rpcErr != nil {
		t.Fatalf("SendMessage: err=%v rpc=%+v", err, rpcErr)
	}
	done, err := c.WaitTerminal(context.Background(), sent.ID, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitTerminal: %v", err)
	}
	if done.ID != sent.ID || done.Status.State != StateCompleted {
		t.Fatalf("WaitTerminal → id=%s state=%s, want %s/completed", done.ID, done.Status.State, sent.ID)
	}

	// 4-b: dm.read는 ask_owner → input-required. 터미널이 아니므로 폴링이 계속되고,
	// caller가 ctx를 만료시키면 그 에러로 중단된다(사람 개입 대기의 클라이언트 측 모형).
	holding, _, _ := c.SendMessage(context.Background(), MessageSendParams{
		Tenant:   "lumi",
		Message:  Message{Role: "agent", Parts: []Part{TextPart("digest recent DMs")}},
		Metadata: map[string]any{"skill": "dm.read"},
	})
	if holding.Status.State != StateInputRequired {
		t.Fatalf("setup: dm.read should be input-required, got %s", holding.Status.State)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := c.WaitTerminal(ctx, holding.ID, 20*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("input-required polling must end with ctx deadline, got %v", err)
	}
}

// ⑤ A2A-Version 응답 처리 — 클라이언트는 헤더를 싣지만, 원격 경로에서 헤더가
// 벗겨지면 bridge는 -32009로 응답하고, 클라이언트는 이를 (nil, *RPCError, nil)로 노출.
func TestA2AClientVersionNotSupported(t *testing.T) {
	e := startA2ARemote(t)
	strip := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Del("A2A-Version")
		e.br.Handler().ServeHTTP(w, r)
	})
	srv := httptest.NewServer(strip)
	t.Cleanup(srv.Close)
	c := &A2AClient{BaseURL: srv.URL, Token: e.tok}

	task, rpcErr, err := c.SendMessage(context.Background(), sumParams())
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if task != nil {
		t.Fatalf("task must be nil on RPCError, got %+v", task)
	}
	if rpcErr == nil || rpcErr.Code != CodeVersionNotSup {
		t.Fatalf("want RPCError %d, got %+v", CodeVersionNotSup, rpcErr)
	}
}

// ⑥ 비준수 원격 — result로 Task가 아닌 Message를 반환하면 역직렬화 실패 error
// (빈 Task를 성공으로 돌려주지 않는다).
func TestA2AClientRejectsNonTaskResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, RPCResponse{
			JSONRPC: "2.0",
			ID:      json.RawMessage("1"),
			Result:  Message{Role: "agent", Parts: []Part{TextPart("i am a message, not a task")}},
		})
	}))
	t.Cleanup(srv.Close)
	c := &A2AClient{BaseURL: srv.URL, Token: "any"}

	task, rpcErr, err := c.SendMessage(context.Background(), MessageSendParams{Tenant: "lumi"})
	if err == nil {
		t.Fatal("want error when result is not a Task")
	}
	if task != nil || rpcErr != nil {
		t.Fatalf("task/rpcErr must be nil: task=%+v rpc=%+v", task, rpcErr)
	}
	if !strings.Contains(err.Error(), "not a Task") {
		t.Errorf("error should say result is not a Task: %v", err)
	}
}

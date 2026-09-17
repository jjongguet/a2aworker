package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestHubJoinPollResult — 초대코드 → join → dispatch → poll 수령 → result 보고 → 회수.
// ACP 실행 없이 허브의 전체 라이프사이클을 HTTP로 검증한다.
func TestHubJoinPollResult(t *testing.T) {
	hub := NewHub()
	mux := http.NewServeMux()
	mux.HandleFunc("/hub/invite", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			WorkerID string   `json:"workerId"`
			Skills   []string `json:"skills"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		inv, err := hub.MintInvite(req.WorkerID, req.Skills, time.Hour)
		if err != nil {
			t.Fatalf("mint invite: %v", err)
		}
		writeJSON(w, http.StatusOK, map[string]any{"code": inv.Code})
	})
	mux.HandleFunc("/hub/join", hub.serveJoin)
	mux.HandleFunc("/hub/poll", func(w http.ResponseWriter, r *http.Request) { hub.servePoll(w, r) })
	mux.HandleFunc("/hub/result", hub.serveResult)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	post := func(path string, body any, token string) (int, []byte) {
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", srv.URL+path, bytes.NewReader(b))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		defer resp.Body.Close()
		raw := new(bytes.Buffer)
		raw.ReadFrom(resp.Body)
		return resp.StatusCode, raw.Bytes()
	}

	// 1) 초대코드 발급
	st, raw := post("/hub/invite", map[string]any{"workerId": "gjc", "skills": []string{"code.review"}}, "")
	if st != 200 {
		t.Fatalf("invite: %d %s", st, raw)
	}
	var inv struct{ Code string }
	json.Unmarshal(raw, &inv)

	// 2) 잘못된 코드는 거절
	st, _ = post("/hub/join", map[string]string{"code": "a2ainv-wrong"}, "")
	if st != 403 {
		t.Fatalf("wrong code should 403, got %d", st)
	}

	// 3) 정상 join
	st, raw = post("/hub/join", map[string]string{"code": inv.Code}, "")
	if st != 200 {
		t.Fatalf("join: %d %s", st, raw)
	}
	var joined struct {
		Token    string `json:"token"`
		WorkerID string `json:"workerId"`
	}
	json.Unmarshal(raw, &joined)
	if joined.Token == "" || joined.WorkerID != "gjc" {
		t.Fatalf("join response incomplete: %s", raw)
	}

	// 4) 같은 코드 재사용 거절 (1회용)
	st, _ = post("/hub/join", map[string]string{"code": inv.Code}, "")
	if st != 403 {
		t.Fatalf("reuse should 403, got %d", st)
	}

	// 5) dispatch → poll로 작업 수령
	jobID, err := hub.Dispatch("gjc", "code.review", "review this", nil)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	st, raw = post("/hub/poll", nil, joined.Token)
	if st != 200 || !strings.Contains(string(raw), jobID) {
		t.Fatalf("poll should return dispatched job: %d %s", st, raw)
	}

	// 6) 결과 보고 → ResultOf로 회수
	st, _ = post("/hub/result", resultReq{JobID: jobID, State: StateCompleted, Output: "LGTM"}, joined.Token)
	if st != 200 {
		t.Fatalf("result report failed: %d", st)
	}
	res, ok := hub.ResultOf(jobID)
	if !ok || res != "completed|LGTM" {
		t.Fatalf("result mismatch: %q ok=%v", res, ok)
	}
}

// TestHubWorkerAuthRequired — 토큰 없이 poll/result는 401.
func TestHubWorkerAuthRequired(t *testing.T) {
	hub := NewHub()
	rec := httptest.NewRecorder()
	hub.servePoll(rec, httptest.NewRequest("GET", "/hub/poll", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("poll without token should 401, got %d", rec.Code)
	}
}

// TestHubDispatchUnknownTenant — 미가입 tenant dispatch는 에러.
func TestHubDispatchUnknownTenant(t *testing.T) {
	hub := NewHub()
	if _, err := hub.Dispatch("nobody", "code.review", "x", nil); err == nil {
		t.Fatal("dispatch to unknown worker should error")
	}
}

// TestHubResultClearsRedeliveredPending — 재배일 레이스 회귀: 실행이 길어
// RedeliverStuck이 PendingJob에 잡을 재진열한 뒤 결과가 보고되면, 다음 poll에서
// 같은 잡이 다시 배달되지 않는다(2026-08-26 종단 검증에서 15분 잡이 두 번
// 실행됐던 원인 — 결과 보고가 PendingJob을 못 치워 즉시 재수행됐음).
func TestHubResultClearsRedeliveredPending(t *testing.T) {
	hub := NewHub()
	mux := http.NewServeMux()
	mux.HandleFunc("/hub/join", hub.serveJoin)
	mux.HandleFunc("/hub/poll", hub.servePoll)
	mux.HandleFunc("/hub/result", hub.serveResult)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	inv, err := hub.MintInvite("w1", []string{"code.review"}, time.Hour)
	if err != nil {
		t.Fatalf("mint invite: %v", err)
	}
	b, _ := json.Marshal(map[string]string{"code": inv.Code})
	resp, err := http.Post(srv.URL+"/hub/join", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	var joined struct {
		Token string `json:"token"`
	}
	json.NewDecoder(resp.Body).Decode(&joined)
	resp.Body.Close()
	if joined.Token == "" {
		t.Fatal("join did not return a token")
	}

	jobID, err := hub.Dispatch("w1", "code.review", "slow job", nil)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// 1) poll로 수령(실제 워커의 실행 단계에 해당)
	got, err := hubPollBody(srv.URL, joined.Token, 5*time.Second)
	if err != nil {
		t.Fatalf("poll #1: %v", err)
	}
	if !strings.Contains(got, jobID) {
		t.Fatalf("poll #1 should deliver %s, got %s", jobID, got)
	}

	// 2) 실행 중 재배일 재현: 결과 없는 잡을 PendingJob에 재진열
	if n := hub.RedeliverStuck(0); n != 1 {
		t.Fatalf("RedeliverStuck should re-pend 1 job, got %d", n)
	}

	// 3) 결과 보고
	rb, _ := json.Marshal(resultReq{JobID: jobID, State: StateCompleted, Output: "done"})
	req, _ := http.NewRequest("POST", srv.URL+"/hub/result", bytes.NewReader(rb))
	req.Header.Set("Authorization", "Bearer "+joined.Token)
	req.Header.Set("Content-Type", "application/json")
	rresp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("result report: %v", err)
	}
	raw, _ := io.ReadAll(rresp.Body)
	rresp.Body.Close()
	if rresp.StatusCode != http.StatusOK {
		t.Fatalf("result report: %d %s", rresp.StatusCode, raw)
	}

	// 4) 다음 poll은 재진열된 잡을 즉시 되돌리면 안 된다. 정상이면 long-poll이
	// 대기로 들어가므로 짧은 클라이언트 타임아웃으로 "즉시 재배달 없음"을 증명.
	if body, err := hubPollBody(srv.URL, joined.Token, 1500*time.Millisecond); err == nil {
		t.Fatalf("job must not redeliver after result report, got: %s", body)
	}
	if res, ok := hub.ResultOf(jobID); !ok || res != "completed|done" {
		t.Fatalf("result mismatch: %q ok=%v", res, ok)
	}
}

// hubPollBody — Bearer poll 1회. timeout에 걸리면 에러를 반환한다.
func hubPollBody(base, token string, timeout time.Duration) (string, error) {
	client := &http.Client{Timeout: timeout}
	req, _ := http.NewRequest("GET", base+"/hub/poll", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw), nil
}

// TestHubOperatorEndpointsRequireAdmin — RCE 회귀 검증: 운영자 면(invite/
// dispatch/result/<id>/workers)은 admin Bearer 없으면 401, adminToken 미설정
// 허브에서는 403으로 폐쇄된다. worker 면(join)은 영향 없음.
func TestHubOperatorEndpointsRequireAdmin(t *testing.T) {
	hub := NewHub()
	srv := httptest.NewServer(hub.Handler("op-secret"))
	defer srv.Close()

	req := func(method, path string, body any, token string) (int, string) {
		var rd io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = bytes.NewReader(b)
		}
		r, _ := http.NewRequest(method, srv.URL+path, rd)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}

	// 토큰 없음/틀림 → 401, 정답 → 200
	ops := []struct {
		method, path string
		body         any
	}{
		{"POST", "/hub/invite", map[string]any{"workerId": "gjc"}},
		{"POST", "/hub/dispatch", map[string]any{"tenant": "gjc", "prompt": "rm -rf /"}},
		{"GET", "/hub/workers", nil},
		{"GET", "/hub/result/job-0001", nil},
	}
	for _, op := range ops {
		if st, _ := req(op.method, op.path, op.body, ""); st != http.StatusUnauthorized {
			t.Fatalf("%s %s without admin token should 401, got %d", op.method, op.path, st)
		}
		if st, _ := req(op.method, op.path, op.body, "wrong"); st != http.StatusUnauthorized {
			t.Fatalf("%s %s with wrong token should 401, got %d", op.method, op.path, st)
		}
	}
	if st, raw := req("POST", "/hub/invite", map[string]any{"workerId": "gjc"}, "op-secret"); st != http.StatusOK {
		t.Fatalf("invite with admin token should 200, got %d %s", st, raw)
	}
	// dispatch는 admin 통과 후 비즈니스 검증(미가입 tenant)까지 도달해야 한다.
	if st, raw := req("POST", "/hub/dispatch", map[string]any{"tenant": "gjc", "prompt": "x"}, "op-secret"); st != http.StatusBadRequest || !strings.Contains(raw, "no worker joined") {
		t.Fatalf("dispatch with admin token should reach Dispatch (400 unknown tenant), got %d %s", st, raw)
	}

	// fail-closed: adminToken 없는 허브는 어떤 토큰을 줘도 운영자 면 403.
	closed := httptest.NewServer(hub.Handler(""))
	defer closed.Close()
	body, _ := json.Marshal(map[string]any{"workerId": "gjc"})
	cr, _ := http.NewRequest("POST", closed.URL+"/hub/invite", bytes.NewReader(body))
	cr.Header.Set("Authorization", "Bearer op-secret")
	cresp, err := http.DefaultClient.Do(cr)
	if err != nil {
		t.Fatalf("closed-hub invite: %v", err)
	}
	craw, _ := io.ReadAll(cresp.Body)
	cresp.Body.Close()
	if cresp.StatusCode != http.StatusForbidden {
		t.Fatalf("hub without A2A_ADMIN_TOKEN must 403 operator endpoints, got %d %s", cresp.StatusCode, craw)
	}
	// worker 면(join)은 폐쇄 없이 동작 — 라우팅 살아있음 증명(잘못 코드 → join 자체의 403).
	jr, _ := http.NewRequest("POST", closed.URL+"/hub/join", bytes.NewReader([]byte(`{"code":"a2ainv-wrong"}`)))
	jresp, err := http.DefaultClient.Do(jr)
	if err != nil {
		t.Fatalf("closed-hub join: %v", err)
	}
	jraw, _ := io.ReadAll(jresp.Body)
	jresp.Body.Close()
	if jresp.StatusCode != http.StatusForbidden || !strings.Contains(string(jraw), "unknown invite code") {
		t.Fatalf("join plane must stay open (invite-code auth), got %d %s", jresp.StatusCode, jraw)
	}
}

// TestHubRejoinRevokesOldToken — 같은 workerID의 재가입은 옛 토큰을 폐기한다.
// 예전엔 TokenHash를 비교하지 않아 옛 토큰이 영구 유효했다.
func TestHubRejoinRevokesOldToken(t *testing.T) {
	hub := NewHub()
	srv := httptest.NewServer(hub.Handler(""))
	defer srv.Close()

	join := func() string {
		inv, err := hub.MintInvite("w1", []string{"code.review"}, time.Hour)
		if err != nil {
			t.Fatalf("mint invite: %v", err)
		}
		b, _ := json.Marshal(map[string]string{"code": inv.Code})
		resp, err := http.Post(srv.URL+"/hub/join", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("join: %v", err)
		}
		var out struct {
			Token string `json:"token"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if out.Token == "" {
			t.Fatal("join did not return a token")
		}
		return out.Token
	}

	tokA := join()
	tokB := join() // 재가입 — tokA는 폐기되어야 한다

	if _, err := hub.Dispatch("w1", "code.review", "x", nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// 옛 토큰은 거절(hubPollBody는 상태코드를 안 돌려주므로 401 본문으로 판정)
	bodyA, err := hubPollBody(srv.URL, tokA, 2*time.Second)
	if err != nil {
		t.Fatalf("poll with old token: %v", err)
	}
	if !strings.Contains(bodyA, "worker auth failed") {
		t.Fatalf("old token must be rejected after re-join, got %s", bodyA)
	}
	// 새 토큰은 잡을 수령
	body, err := hubPollBody(srv.URL, tokB, 5*time.Second)
	if err != nil {
		t.Fatalf("poll with new token: %v", err)
	}
	if !strings.Contains(body, "job-0001") {
		t.Fatalf("new token should receive job, got %s", body)
	}
}

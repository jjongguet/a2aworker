// worker_test.go — runACPAgent 헤드리스 회귀 검증(가짜 ACP 바이너리, 완전 오프라인).
// ①권한 모드 env 주입(2026-08-26 종단 검증: prompt 모드에서 bash 툴이
// session/request_permission 무응답으로 15분 하드킬까지 막혔던 것)
// ②운영자 명시 모드 존중 ③침묵 자식에서 prompt 데드라인 실제 발화.
package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// acpStub — stdin의 JSON-RPC 메서드에 따라 응답을 흉내내는 sh 스크립트.
// 최종 agent_message_chunk에 자신이 받은 GJC_ACP_PERMISSION_MODE 값을 실어
// 돌려보낸다. A2A_TEST_ACP_MODE=silent면 session/prompt 이후 침묵(교착 재현).
func acpStub(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "acp-stub")
	script := `#!/bin/sh
while IFS= read -r line; do
	case "$line" in
	*"initialize"*)
		printf '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}\n' ;;
	*"session/new"*)
		printf '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"stub-sess"}}\n' ;;
	*"session/prompt"*)
		if [ "$A2A_TEST_ACP_MODE" = "silent" ]; then sleep 60; exit 0; fi
		printf '{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"perm=%s"}}}}\n' "$GJC_ACP_PERMISSION_MODE"
		printf '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}\n' ;;
	esac
done
`
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	return p
}

func TestRunACPAgentInjectsAlwaysAllow(t *testing.T) {
	t.Setenv("A2A_TEST_ACP_MODE", "echo")
	out, err := runACPAgent(acpStub(t), "ping")
	if err != nil {
		t.Fatalf("runACPAgent: %v", err)
	}
	if !strings.Contains(out, "perm=always-allow") {
		t.Fatalf("child env should default GJC_ACP_PERMISSION_MODE=always-allow, got output %q", out)
	}
}

func TestRunACPAgentRespectsExplicitPermissionMode(t *testing.T) {
	t.Setenv("A2A_TEST_ACP_MODE", "echo")
	t.Setenv("GJC_ACP_PERMISSION_MODE", "prompt")
	out, err := runACPAgent(acpStub(t), "ping")
	if err != nil {
		t.Fatalf("runACPAgent: %v", err)
	}
	if !strings.Contains(out, "perm=prompt") {
		t.Fatalf("explicit operator mode must be preserved, got output %q", out)
	}
}

func TestRunACPAgentPromptDeadlineFires(t *testing.T) {
	t.Setenv("A2A_TEST_ACP_MODE", "silent")
	orig := acpPromptDeadline
	acpPromptDeadline = 300 * time.Millisecond
	t.Cleanup(func() { acpPromptDeadline = orig })

	start := time.Now()
	_, err := runACPAgent(acpStub(t), "ping")
	elapsed := time.Since(start)
	if err == nil || !strings.Contains(err.Error(), "acp prompt timeout") {
		t.Fatalf("want acp prompt timeout error, got %v", err)
	}
	// 예전 구조는 ReadString 블로킹 때문에 이 데드라인이 절대 안 터졌다.
	if elapsed > 10*time.Second {
		t.Fatalf("deadline should fire promptly, took %v", elapsed)
	}
}

// TestPollOnceAuthRejected — hub가 토큰을 401로 거절하면 일시 오류가 아니라
// errAuthRejected로 전파된다(예전엔 그냥 "poll error"로 5초마다 영구 스픈).
func TestPollOnceAuthRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "worker auth failed"})
	}))
	defer srv.Close()
	_, err := pollOnce(http.DefaultClient, srv.URL, &WorkerConfig{WorkerID: "w1"})
	if !errors.Is(err, errAuthRejected) {
		t.Fatalf("401 poll should surface errAuthRejected, got %v", err)
	}
}

// TestReportResultRetryRecovers — 일시 실패(500) 뒤 성공: 결과를 버리지 않고
// 재시도로 보고한다(1회 실패로 버리면 hub가 재배일해 이중 실행된다).
func TestReportResultRetryRecovers(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	}))
	defer srv.Close()

	orig := reportRetryBackoff
	reportRetryBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { reportRetryBackoff = orig })

	if err := reportResultRetry(http.DefaultClient, srv.URL, &WorkerConfig{WorkerID: "w1"}, "job-0001", StateCompleted, "done"); err != nil {
		t.Fatalf("retry should recover after transient failures: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("want 3 attempts, got %d", attempts)
	}
}

// TestReportResultRetryAuthFailsFast — 401은 재시도해도 회복되지 않으므로 1회에 그친다.
func TestReportResultRetryAuthFailsFast(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "worker auth failed"})
	}))
	defer srv.Close()

	if err := reportResultRetry(http.DefaultClient, srv.URL, &WorkerConfig{WorkerID: "w1"}, "job-0001", StateCompleted, "done"); !errors.Is(err, errAuthRejected) {
		t.Fatalf("want errAuthRejected, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("auth rejection must not be retried, got %d attempts", attempts)
	}
}

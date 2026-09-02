// executor_test.go — 실전 어댑터(HermesExecutor)를 가짜 hermes 바이너리(shim)로
// 오프라인 검증한다. 실 VM/실 fleet 무접촉.
//
// 증명 항목:
//  1. argv 형태: hermes -p <profile> -z [--max-budget-usd X] "<프롬프트 verbatim>"
//  2. stdout 최종답이 그대로 Task artifact로 흐름 (bridge 통합)
//  3. shim 실패 시 Task → failed 상태 매핑
//  4. 승인(approve) 경로도 동일 executor 사용
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeShim — 가짜 hermes 바이너리: argv를 파일에 기록하고 고정 응답 출력.
// mode: "ok" | "fail"
func writeShim(t *testing.T, dir, mode string) (bin string, argsFile string) {
	t.Helper()
	bin = filepath.Join(dir, "fake-hermes")
	argsFile = filepath.Join(dir, "argv.json")
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$@" > %[2]q.tmp
mv %[2]q.tmp %[2]q
if [ "%[1]s" = "fail" ]; then
  echo "auth error: provider token expired" >&2
  exit 1
fi
echo "[fake-lumi] summarized via shim"
`, mode, argsFile)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, argsFile
}

func TestHermesExecutorArgvAndStdout(t *testing.T) {
	dir := t.TempDir()
	bin, argsFile := writeShim(t, dir, "ok")
	e := &HermesExecutor{Bin: bin, Timeout: 10 * time.Second, MaxBudget: "0.50"}

	out, err := e.Execute("lumi", "logs.summarize", Message{Parts: []Part{TextPart("summarize yesterday's fleet logs")}}, "trace-test")
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out != "[fake-lumi] summarized via shim" {
		t.Fatalf("unexpected stdout: %q", out)
	}

	raw, _ := os.ReadFile(argsFile)
	argv := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	// argv = [-p lumi -z --max-budget-usd 0.50 <prompt>]
	joined := strings.Join(argv, "\x00")
	for _, want := range []string{"-p\x00lumi", "-z", "--max-budget-usd\x000.50", "summarize yesterday's fleet logs"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("argv missing %q: %v", want, argv)
		}
	}
	if argv[len(argv)-1] != "summarize yesterday's fleet logs" {
		t.Fatalf("prompt must be verbatim and last: %v", argv)
	}
}

// bridgeWithExecutor — 실전 실행기가 주입된 bridge 테스트 환경.
func bridgeWithExecutor(t *testing.T, e Executor) (*httptest.Server, string) {
	t.Helper()
	bots := map[string]*Bot{"jena": NewBot("jena"), "lumi": NewBot("lumi")}
	br := NewBridge("", bots)
	br.SetExecutor(e)
	for _, b := range bots {
		b.SetBridgeSecret(string(br.InternalSecret()))
	}
	srv := httptest.NewServer(br.Handler())
	t.Cleanup(srv.Close)
	return srv, br.MintClientToken("jena", []string{"logs.summarize", "dm.read"})
}

func TestHermesExecutorIntegrationOK(t *testing.T) {
	dir := t.TempDir()
	bin, _ := writeShim(t, dir, "ok")
	srv, tok := bridgeWithExecutor(t, &HermesExecutor{Bin: bin, Timeout: 10 * time.Second})

	_, task, r := rpc(t, srv.URL, "SendMessage", tok, sendParams("lumi", "logs.summarize"))
	if r.Error != nil {
		t.Fatalf("rpc error: %+v", r.Error)
	}
	if task.Status.State != StateCompleted || !strings.Contains(task.Artifacts[0].Parts[0].Text, "via shim") {
		t.Fatalf("want shim output in completed task: state=%s msg=%+v", task.Status.State, task.Status.Message)
	}
}

func TestHermesExecutorFailureMapsToTaskFailed(t *testing.T) {
	dir := t.TempDir()
	bin, _ := writeShim(t, dir, "fail")
	srv, tok := bridgeWithExecutor(t, &HermesExecutor{Bin: bin, Timeout: 10 * time.Second})

	_, task, r := rpc(t, srv.URL, "SendMessage", tok, sendParams("lumi", "logs.summarize"))
	if r.Error != nil {
		t.Fatalf("rpc-level error unexpected (should be task state): %+v", r.Error)
	}
	if task.Status.State != StateFailed || !strings.Contains(task.Status.Message.Parts[0].Text, "executor error") {
		t.Fatalf("want failed state with executor error: state=%s msg=%+v", task.Status.State, task.Status.Message)
	}
}

func TestOwnerApprovePathUsesExecutor(t *testing.T) {
	dir := t.TempDir()
	bin, argsFile := writeShim(t, dir, "ok")
	srv, tok := bridgeWithExecutor(t, &HermesExecutor{Bin: bin, Timeout: 10 * time.Second})

	_, task, _ := rpc(t, srv.URL, "SendMessage", tok, sendParams("lumi", "dm.read"))
	if task.Status.State != StateInputRequired {
		t.Fatalf("want input-required, got %s", task.Status.State)
	}
	approve := MessageSendParams{
		Tenant: "lumi", Message: Message{Role: "user", Parts: []Part{TextPart("approved")}},
		Metadata: map[string]any{"taskId": task.ID, "owner_decision": "approve"},
	}
	_, done, ra := rpc(t, srv.URL, "SendMessage", tok, approve)
	if ra.Error != nil || done.Status.State != StateCompleted || !strings.Contains(done.Status.Message.Parts[0].Text, "owner-approved") {
		t.Fatalf("approve flow: state=%s err=%+v", done.Status.State, ra.Error)
	}
	// 승인 경로 호출이 실제 shim을 거쳤는지(argv 파일 갱신) 확인
	raw, _ := os.ReadFile(argsFile)
	if !strings.Contains(string(raw), "-p") {
		t.Fatalf("approve path did not invoke executor: %q", string(raw))
	}
}

var _ = http.StatusOK

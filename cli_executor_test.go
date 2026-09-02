// cli_executor_test.go — 코딩에이전트 provider 모드 검증. 가짜 CLI 바이너리(shim)로
// ①기본 템플릿 argv(gjc/claude/codex) ②bridge 통합 ③code.implement ask_owner
// ④미등록 tenant ⑤다중 소유자 정책 — 전부 오프라인.
package main

import (
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// cliShim — writeShim(executor_test.go) 재사용: 호출마다 별도 dir에 바이너리 생성.
func cliShim(t *testing.T, mode string) string {
	t.Helper()
	bin, _ := writeShim(t, t.TempDir(), mode)
	return bin
}

func TestCLIDefaultTemplatesArgv(t *testing.T) {
	cases := []struct {
		tenant string
		want   string
	}{
		{"gjc", "-p\x00--no-session\x00--tools\x00read,search,find"},
		{"claude", "-p"},
		{"codex", "exec"},
	}
	prompt := "review src/auth for session handling"
	for _, tc := range cases {
		t.Run(tc.tenant, func(t *testing.T) {
			dir := t.TempDir()
			bin, argsFile := writeShim(t, dir, "ok")
			e := &CLIExecutor{Cmds: DefaultCLITemplates(), Timeout: 10 * time.Second}
			e.Cmds[tc.tenant] = append([]string{bin}, e.Cmds[tc.tenant][1:]...) // 바이너리만 교체

			out, err := e.Execute(tc.tenant, "code.review", Message{Parts: []Part{TextPart(prompt)}}, "trace-test")
			if err != nil || !strings.Contains(out, "via shim") {
				t.Fatalf("%s: out=%q err=%v", tc.tenant, out, err)
			}
			raw, _ := os.ReadFile(argsFile)
			argv := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
			joined := strings.Join(argv, "\x00")
			if !strings.Contains(joined, tc.want) {
				t.Fatalf("%s argv missing %q: %v", tc.tenant, tc.want, argv)
			}
			if argv[len(argv)-1] != prompt {
				t.Fatalf("%s prompt must be verbatim last arg: %v", tc.tenant, argv)
			}
		})
	}
}

// cliBridge — 코딩에이전트 tenant를 포함한 bridge 테스트 환경.
func cliBridge(t *testing.T, e Executor) (*httptest.Server, string) {
	t.Helper()
	bots := map[string]*Bot{
		"jena": NewBot("jena"), "lumi": NewBot("lumi"),
		"gjc": NewBot("gjc"), "claude": NewBot("claude"), "codex": NewBot("codex"),
	}
	br := NewBridge("", bots)
	br.SetExecutor(e)
	for _, b := range bots {
		b.SetBridgeSecret(string(br.InternalSecret()))
	}
	srv := httptest.NewServer(br.Handler())
	t.Cleanup(srv.Close)
	return srv, br.MintClientToken("jena", []string{"code.review", "code.implement", "logs.summarize"})
}

func TestCLIExecutorBridgeIntegration(t *testing.T) {
	srv, tok := cliBridge(t, &CLIExecutor{
		Cmds:    map[string][]string{"claude": {cliShim(t, "ok"), "-p"}},
		Timeout: 10 * time.Second,
	})
	_, task, r := rpc(t, srv.URL, "SendMessage", tok, sendParams("claude", "code.review"))
	if r.Error != nil {
		t.Fatalf("rpc error: %+v", r.Error)
	}
	if task.Status.State != StateCompleted || !strings.Contains(task.Artifacts[0].Parts[0].Text, "via shim") {
		t.Fatalf("want shim output: state=%s msg=%+v", task.Status.State, task.Status.Message)
	}
}

func TestCLICodeImplementAskOwner(t *testing.T) {
	dir := t.TempDir()
	bin, argsFile := writeShim(t, dir, "ok")
	srv, tok := cliBridge(t, &CLIExecutor{
		Cmds:    map[string][]string{"gjc": {bin, "-p", "--no-session"}},
		Timeout: 10 * time.Second,
	})
	_, task, _ := rpc(t, srv.URL, "SendMessage", tok, sendParams("gjc", "code.implement"))
	if task.Status.State != StateInputRequired {
		t.Fatalf("code.implement must be ask_owner, got %s", task.Status.State)
	}
	approve := MessageSendParams{
		Tenant: "gjc", Message: Message{Role: "user", Parts: []Part{TextPart("approved")}},
		Metadata: map[string]any{"taskId": task.ID, "owner_decision": "approve"},
	}
	_, done, ra := rpc(t, srv.URL, "SendMessage", tok, approve)
	if ra.Error != nil || done.Status.State != StateCompleted {
		t.Fatalf("approve: state=%s err=%+v", done.Status.State, ra.Error)
	}
	if raw, _ := os.ReadFile(argsFile); !strings.Contains(string(raw), "--no-session") {
		t.Fatalf("approve path did not invoke gjc shim: %q", string(raw))
	}
}

func TestCLIUnknownTenantFails(t *testing.T) {
	srv, tok := cliBridge(t, NewCLIExecutor()) // lumi엔 CLI 템플릿 없음
	_, task, r := rpc(t, srv.URL, "SendMessage", tok, sendParams("lumi", "logs.summarize"))
	if r.Error != nil {
		t.Fatalf("rpc-level error unexpected: %+v", r.Error)
	}
	if task.Status.State != StateFailed || !strings.Contains(task.Status.Message.Parts[0].Text, "no CLI template") {
		t.Fatalf("want failed/no CLI template: state=%s msg=%+v", task.Status.State, task.Status.Message)
	}
}

func TestCLIMultiOwnerPolicy(t *testing.T) {
	srv, tok := cliBridge(t, NewCLIExecutor())
	// jena(Hermes 봇)는 code.review를 서빙하지 않음 → 정책 거절
	_, task, r := rpc(t, srv.URL, "SendMessage", tok, sendParams("jena", "code.review"))
	if r.Error != nil {
		t.Fatalf("rpc error: %+v", r.Error)
	}
	if task.Status.State != StateRejected || !strings.Contains(task.Status.Message.Parts[0].Text, "not served by") {
		t.Fatalf("want rejected/not-served-by: state=%s msg=%+v", task.Status.State, task.Status.Message)
	}
}

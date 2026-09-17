// worker.go — 워커 모드: "남의 PC"에서 한 줄로 실행하는 에이전트.
// 아웃바운드 HTTP 폴링만 한다(인바운드 포트 0, SSH 불필요 — NAT 뒤 OK).
//
//	a2aworker join --hub https://hub.example --invite a2ainv-xxxx
//	  → 초대코드를 worker 토큰으로 교환 → ~/.a2aworker/worker.json 저장
//	a2aworker work --hub https://hub.example
//	  → long-poll 루프: 작업 수령 → 로컬 ACP 에이전트(gjc acp) 실행 → 결과 보고
//
// 로컬 에이전트 호출은 ACP stdio(공식 문서 검증 패턴):
//
//	initialize → session/new → session/prompt → (응답 수집) → session/end 생략 가능.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// WorkerConfig — ~/.a2aworker/worker.json에 저장되는 것.
type WorkerConfig struct {
	Hub      string   `json:"hub"`
	Token    string   `json:"token"`
	WorkerID string   `json:"workerId"`
	Skills   []string `json:"skills"`
}

func workerConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".a2aworker")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return filepath.Join(dir, "worker.json"), nil
}

func saveWorkerConfig(c *WorkerConfig) error {
	p, err := workerConfigPath()
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(p, b, 0o600)
}

func loadWorkerConfig() (*WorkerConfig, error) {
	p, err := workerConfigPath()
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("no worker config at %s — run 'join' first", p)
	}
	var c WorkerConfig
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// ---- join ----

func runJoin(hubURL, inviteCode string) error {
	body, _ := json.Marshal(map[string]string{"code": inviteCode, "host": hostnameLabel()})
	resp, err := http.Post(strings.TrimRight(hubURL, "/")+"/hub/join", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("join %s: %w", hubURL, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join rejected: %s (%s)", strings.TrimSpace(string(raw)), resp.Status)
	}
	var out struct {
		Token    string   `json:"token"`
		WorkerID string   `json:"workerId"`
		Skills   []string `json:"skills"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return err
	}
	cfg := &WorkerConfig{Hub: strings.TrimRight(hubURL, "/"), Token: out.Token, WorkerID: out.WorkerID, Skills: out.Skills}
	if err := saveWorkerConfig(cfg); err != nil {
		return err
	}
	fmt.Printf("joined as %q (skills: %v)\nconfig saved to ~/.a2aworker/worker.json\nnext: a2aworker work\n", out.WorkerID, out.Skills)
	return nil
}

func hostnameLabel() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown-host"
	}
	return h
}

// ---- work (long-poll loop) ----

type HubJobWire struct {
	ID       string         `json:"id,omitempty"`
	Tenant   string         `json:"tenant,omitempty"`
	Skill    string         `json:"skill,omitempty"`
	Prompt   string         `json:"prompt,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
	None     bool           `json:"none,omitempty"` // poll 빈 응답
	Error    string         `json:"error,omitempty"`
}

func runWork(hubOverride string, acpBin string, interval time.Duration) error {
	cfg, err := loadWorkerConfig()
	if err != nil {
		return err
	}
	hub := cfg.Hub
	if hubOverride != "" {
		hub = strings.TrimRight(hubOverride, "/")
	}
	client := &http.Client{Timeout: 40 * time.Second}
	fmt.Printf("working: hub=%s worker=%s acp=%s\n", hub, cfg.WorkerID, acpBin)
	for {
		job, err := pollOnce(client, hub, cfg)
		if err != nil {
			if errors.Is(err, errAuthRejected) {
				return fmt.Errorf("worker token rejected by hub (hub restarted or worker re-joined) — re-join required: %w", err)
			}
			fmt.Println("poll error:", err)
			time.Sleep(interval)
			continue
		}
		if job == nil || job.None || job.ID == "" {
			continue // long-polled empty — 즉시 재폴링
		}
		fmt.Printf("job %s received: skill=%s prompt=%.60q\n", job.ID, job.Skill, job.Prompt)
		out, execErr := runACPAgent(acpBin, job.Prompt)
		state, text := StateCompleted, out
		if execErr != nil {
			state, text = StateFailed, execErr.Error()
		}
		if err := reportResultRetry(client, hub, cfg, job.ID, state, text); err != nil {
			// 결과가 hub에 못 닿으면 hub는 이 잡을 재배일한다(maxRetry까지) —
			// 부수효과 있는 프롬프트의 중복 실행 위험을 알린다.
			fmt.Printf("job %s report FAILED after retries: %v — hub will redeliver; duplicate execution possible\n", job.ID, err)
		} else {
			fmt.Printf("job %s reported: %s\n", job.ID, state)
		}
	}
}

func pollOnce(client *http.Client, hub string, cfg *WorkerConfig) (*HubJobWire, error) {
	req, _ := http.NewRequest("GET", hub+"/hub/poll", nil)
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("%w: poll http %d: %.120s", errAuthRejected, resp.StatusCode, string(raw))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("poll http %d: %.120s", resp.StatusCode, string(raw))
	}
	var j HubJobWire
	if err := json.Unmarshal(raw, &j); err != nil {
		return nil, err
	}
	return &j, nil
}

func reportResult(client *http.Client, hub string, cfg *WorkerConfig, jobID, state, text string) error {
	body, _ := json.Marshal(resultReq{JobID: jobID, State: state, Output: text})
	req, _ := http.NewRequest("POST", hub+"/hub/result", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("%w: result http %d", errAuthRejected, resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("result http %d: %.120s", resp.StatusCode, string(raw))
	}
	return nil
}

// ---- ACP stdio 클라이언트 (gjc acp / claude 등 공통) ----

// errAuthRejected — 허브가 워커 토큰을 401/403로 거절. 허브 재시작(서명 키
// 재생성) 또는 같은 workerID의 re-join(토큰 폐기)뒤에는 재접속으로만 회복되므로
// 일시 오류로 재시도하지 말고 치명 에러로 전파한다.
var errAuthRejected = errors.New("worker auth rejected")

// reportRetryBackoff — reportResultRetry의 재시도 간격(테스트에서 단축).
var reportRetryBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}

// reportResultRetry — 결과 보고를 백오프 재시도. 1회 실패로 결과를 버리면
// hub는 잡을 미보고로 간주해 재배일하고, 부수효과 있는 프롬프트가 이중 실행된다.
func reportResultRetry(client *http.Client, hub string, cfg *WorkerConfig, jobID, state, text string) error {
	var err error
	for attempt := 0; attempt <= len(reportRetryBackoff); attempt++ {
		if attempt > 0 {
			time.Sleep(reportRetryBackoff[attempt-1])
		}
		if err = reportResult(client, hub, cfg, jobID, state, text); err == nil {
			return nil
		}
		if errors.Is(err, errAuthRejected) {
			return err // 재시도로 회복되지 않는다
		}
	}
	return fmt.Errorf("after %d attempts: %w", len(reportRetryBackoff)+1, err)
}

// runACPAgent — ACP stdio 자식 프로세스를 띄우고 initialize→session/new→prompt로
// 프롬프트를 넣어 최종 텍스트를 모아 반환한다. ACP 메시지는 JSON-RPC 2.0 한 줄씩.
func runACPAgent(bin, prompt string) (string, error) {
	// 좀비 방지: 전체 ACP 세션에 15분 하드 상한(CommandContext → 기한 초과 시
	// 자식 kill). defer Wait로 프로세스 테이블에서 확실히 수거한다.
	argv := strings.Fields(bin)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	// 자식을 새 프로세스 그룹으로: 정리 시 직계 자식뿐 아니라 손자(sh가 fork한
	// sleep, 에이전트가 남긴 서브프로세스)까지 한 번에 죽린다. 그룹 킬이 없으면
	// 살아남은 손자가 상속받은 stdout/stderr 파이프를 붙잡아 상위 실행자(go test
	// 드라이버, 허브 백그라운드 잡)의 종료 감지를 60초+ 지연시켰다.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// 헤드리스 권한: gjc acp 기본 permission mode는 "prompt"라 bash 같은 게이트
	// 툴마다 session/request_permission을 클라이언트에 되묻는다. 응답하지 않는
	// 토이 클라이언트는 타임아웃 없이 영구 대기한다(2026-08-26 종단 검증에서
	// 15분 하드킬까지 막혔던 원인). 워커는 로컬 신뢰 실행이므로 기본을 항상
	// 허용으로 띄운다.
	cmd.Env = acpChildEnv()
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("acp start %s: %w", bin, err)
	}
	// 조기 반환 시에도 cancel로 자식을 즉시 죽인 뒤 Wait한다. 그렇지 않으면
	// 데드라인 에러로 일찍 돌아가도 Wait이 15분 상한까지 묶인다. 그룹 시그널로
	// 손자 프로세스까지 정리한다(파이프 보유로 인한 상위 종료 감시 지연 방지).
	defer func() {
		cancel()
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		stdin.Close()
		cmd.Wait()
	}()

	type acpMsg struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id,omitempty"`
		Method  string          `json:"method,omitempty"`
		Result  json.RawMessage `json:"result,omitempty"`
		Params  map[string]any  `json:"params,omitempty"`
	}
	// 읽기 루프를 별도 고루틴+채널로 분리: 예전 select-default 패턴은
	// ReadString 블로킹 중에는 데드라인 검사에 도달하지 못해 10분 프롬프트
	// 데드라인이 절대 발화하지 않았다(15분 ctx kill이 먼저 터졌음).
	reader := bufio.NewReader(stdout)
	lines := make(chan string)
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer close(lines)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return // EOF 또는 파이프 닫힘
			}
			select {
			case lines <- line:
			case <-done: // 소비자가 먼저 반환하면 남은 줄을 버리고 종료(누수 방지)
			}
		}
	}()
	decode := func(line string) (acpMsg, bool) {
		line = strings.TrimSpace(line)
		if line == "" {
			return acpMsg{}, false
		}
		var m acpMsg
		if json.Unmarshal([]byte(line), &m) != nil {
			return acpMsg{}, false
		}
		return m, true
	}

	send := func(v any) error {
		b, _ := json.Marshal(v)
		_, err := stdin.Write(append(b, '\n'))
		return err
	}
	readResp := func(wantMethod string, timeout time.Duration) (json.RawMessage, error) {
		deadline := time.After(timeout)
		for {
			select {
			case <-deadline:
				return nil, fmt.Errorf("acp timeout waiting for %s response", wantMethod)
			case line, ok := <-lines:
				if !ok {
					return nil, fmt.Errorf("acp read: stream closed waiting for %s response", wantMethod)
				}
				m, valid := decode(line)
				if !valid {
					continue
				}
				if m.ID != nil && m.Result != nil {
					return m.Result, nil // 요청에 대한 응답
				}
				// 알림(session/update 등)은 무시 — 토이 스코프에서는 최종 결과만 수집
			}
		}
	}

	if err := send(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "initialize",
		"params": map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}},
	}); err != nil {
		return "", err
	}
	if _, err := readResp("initialize", 30*time.Second); err != nil {
		return "", err
	}
	if err := send(map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "session/new",
		"params": map[string]any{"cwd": mustCwd(), "mcpServers": []any{}},
	}); err != nil {
		return "", err
	}
	sessRes, err := readResp("session/new", 30*time.Second)
	if err != nil {
		return "", err
	}
	var sess struct {
		SessionID string `json:"sessionId"`
	}
	if json.Unmarshal(sessRes, &sess) != nil || sess.SessionID == "" {
		return "", fmt.Errorf("acp session/new: no sessionId in %.200s", string(sessRes))
	}

	// session/prompt — 최종 stopReason이 올 때까지 update 알림 중 텍스트 수집.

	if err := send(map[string]any{
		"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
		"params": map[string]any{
			"sessionId": sess.SessionID,
			"prompt":    []map[string]any{{"type": "text", "text": prompt}},
		},
	}); err != nil {
		return "", err
	}

	// prompt 응답(stopReason 포함)까지 알림을 소화하며 agent_message 텍스트를 모은다.
	var sb strings.Builder
	promptDeadline := time.After(acpPromptDeadline)
promptLoop:
	for {
		select {
		case <-promptDeadline:
			return "", fmt.Errorf("acp prompt timeout")
		case line, ok := <-lines:
			if !ok {
				return "", fmt.Errorf("acp prompt read: stream closed")
			}
			m, valid := decode(line)
			if !valid {
				continue
			}
			if m.Method == "session/update" {
				u, _ := m.Params["update"].(map[string]any)
				// 실측 wire 포맷(acp-probe 검증): 최종 답변은 agent_message_chunk.
				// agent_thought_chunk는 사고 과정이므로 제외.
				if u != nil && u["sessionUpdate"] == "agent_message_chunk" {
					if t, ok := u["content"].(map[string]any); ok {
						if txt, ok := t["text"].(string); ok {
							sb.WriteString(txt)
						}
					}
				}
				continue
			}
			if m.ID != nil && m.Result != nil { // id=3 prompt 응답 도착
				break promptLoop
			}
		}
	}
	out := strings.TrimSpace(sb.String())
	if out == "" {
		out = "(empty agent response)"
	}
	return out, nil
}

// acpPromptDeadline — prompt 루프 상한. 테스트에서 단축해 발화를 검증한다.
var acpPromptDeadline = 10 * time.Minute

// acpChildEnv — ACP 자식 환경: 상속 + 헤드리스 권한 기본값(always-allow).
// 운영자가 이미 GJC_ACP_PERMISSION_MODE를 지정했다면 그 의도를 존중한다.
func acpChildEnv() []string {
	env := os.Environ()
	for _, kv := range env {
		if strings.HasPrefix(kv, "GJC_ACP_PERMISSION_MODE=") {
			return env
		}
	}
	return append(env, "GJC_ACP_PERMISSION_MODE=always-allow")
}

func mustCwd() string {
	d, err := os.Getwd()
	if err != nil {
		return "."
	}
	return d
}

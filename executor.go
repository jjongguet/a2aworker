// executor.go — 스킬 실행 어댑터. 시뮬레이션과 실전(hermes CLI)을 같은 인터페이스로.
//
// 실전 모드는 "스위치만 켜면" 동작하도록 준비된 상태:
//
//	A2A_EXECUTOR=hermes  → HermesExecutor 가 exec 로 hermes 를 호출
//	(기본 sim: 기존 시뮬레이션, 실전 VM 접촉 없음)
//
// Hermes 호출 형태(운영 가이드 claude-skills/hermes/SKILL.md 기준):
//
//	hermes -p <profile> -z "<프롬프트 verbatim>"
//	-p: 프로필(=tenant=봇) 선택, -z: 프로그램틱 원샷(최종답만 stdout, 승인 bypass)
//	승인 bypass ↔ bridge 검문(DENY/ask_owner/depth)이 승인 책임을 대행하는 구조.
package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Executor — 스킬 실행 추상. profile = Hermes 프로필(=tenant) 이름.
// traceID는 로컬 Task의 위임 추적 ID: federation 실행 시 원격 홉까지 동일하게
// 전파되어 단일 논리 작업으로 상관된다(로컬 실행기는 무시해도 된다).
type Executor interface {
	Execute(profile, skill string, msg Message, traceID string) (string, error)
}

// SimExecutor — 로컬 시뮬레이션(기본). 실 fleet 무접촉.
type SimExecutor struct{}

func (SimExecutor) Execute(_, skill string, msg Message, _ string) (string, error) {
	return ExecuteSkill(skill, msg), nil
}

// HermesExecutor — 실전 어댑터: 프로필 봇을 hermes 원샷 CLI로 호출.
type HermesExecutor struct {
	Bin       string        // 기본 "hermes"
	Timeout   time.Duration // 기본 180s (LLM 응답 대기)
	MaxBudget string        // 예: "0.50" → --max-budget-usd 0.50 (빈 값이면 미지정)
	HomeDir   string        // HERMES_HOME override (예: /root/.hermes). 빈 값이면 상속
}

func NewHermesExecutor() *HermesExecutor {
	return &HermesExecutor{Bin: "hermes", Timeout: 180 * time.Second}
}

// BuildArgs — 실제 exec 인자. 테스트가 argv 형태를 검증하는 대상이기도 함.
func (h *HermesExecutor) BuildArgs(profile, prompt string) []string {
	args := []string{"-p", profile, "-z"}
	if h.MaxBudget != "" {
		args = append(args, "--max-budget-usd", h.MaxBudget)
	}
	return append(args, prompt)
}

func (h *HermesExecutor) Execute(profile, skill string, msg Message, _ string) (string, error) {
	bin := h.Bin
	if bin == "" {
		bin = "hermes"
	}
	timeout := h.Timeout
	if timeout == 0 {
		timeout = 180 * time.Second
	}
	// 가이드 원칙: "Forward verbatim. Don't paraphrase." — 요청 본문을 그대로 전달.
	prompt := textOf(msg)
	if prompt == "" {
		prompt = fmt.Sprintf("skill: %s", skill)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, h.BuildArgs(profile, prompt)...)
	if h.HomeDir != "" {
		cmd.Env = append(cmd.Environ(), "HERMES_HOME="+h.HomeDir)
	}
	out, err := cmd.Output()
	if ctx.Err() == context.DeadlineExceeded {
		return "", fmt.Errorf("hermes call for profile %q timed out after %s", profile, timeout)
	}
	if err != nil {
		return "", fmt.Errorf("hermes call for profile %q failed: %w (stderr may contain auth/config error)", profile, err)
	}
	return strings.TrimSpace(string(out)), nil
}

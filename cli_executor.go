// cli_executor.go — 코딩에이전트 CLI(gjc/claude/codex)를 A2A tenant로 실행하는
// 어댑터. "A2A Provider 우선" 전략의 실행 반쪽: 각 위치의 bridge가 자기 로컬
// 코딩에이전트를 provider(tenant)로 공개하고, 원격 호출자는 표준 A2A 흐름으로 접근.
//
// 기본 템플릿(공식 헤드리스 호출면, 세션 문서/설치 버전 기준):
//
//	gjc    gjc -p --no-session --tools read,search,find "<프롬프트>"
//	       (gajae-code docs/extragoal-skill-template.md의 공식 읽기전용 원샷 패턴)
//	claude claude -p "<프롬프트>"                 (print mode)
//	codex  codex exec "<프롬프트>"                (비대화형 exec)
//
// 쓰기 작업은 정책层에서 막는다: code.implement는 sensitive → 소유주 승인 필요.
package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// CLIExecutor — tenant(프로필)별 명령 템플릿으로 로컬 CLI 에이전트 실행.
type CLIExecutor struct {
	Cmds    map[string][]string // tenant → 기본 argv(마지막에 프롬프트 append)
	Timeout time.Duration
}

// DefaultCLITemplates — 코딩에이전트 3종 기본값.
func DefaultCLITemplates() map[string][]string {
	return map[string][]string{
		"gjc":    {"gjc", "-p", "--no-session", "--tools", "read,search,find"},
		"claude": {"claude", "-p"},
		"codex":  {"codex", "exec", "--skip-git-repo-check"},
	}
}

func NewCLIExecutor() *CLIExecutor {
	return &CLIExecutor{Cmds: DefaultCLITemplates(), Timeout: 600 * time.Second}
}

// BuildArgs — tenant argv에 프롬프트(verbatim)를 붙인 최종 인자.
func (e *CLIExecutor) BuildArgs(tenant, prompt string) ([]string, error) {
	base, ok := e.Cmds[tenant]
	if !ok {
		return nil, fmt.Errorf("no CLI template for tenant %q", tenant)
	}
	return append(append([]string{}, base...), prompt), nil
}

func (e *CLIExecutor) Execute(tenant, skill string, msg Message, _ string) (string, error) {
	timeout := e.Timeout
	if timeout == 0 {
		timeout = 600 * time.Second
	}
	prompt := textOf(msg)
	if prompt == "" {
		prompt = fmt.Sprintf("skill: %s", skill)
	}
	argv, err := e.BuildArgs(tenant, prompt)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	out, err := cmd.Output()
	if ctx.Err() == context.DeadlineExceeded {
		return "", fmt.Errorf("%s call timed out after %s", tenant, timeout)
	}
	if err != nil {
		return "", fmt.Errorf("%s call failed: %w", tenant, err)
	}
	return strings.TrimSpace(string(out)), nil
}

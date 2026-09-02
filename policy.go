// policy.go — 위임 검문소. A2A 스펙이 "agent-defined"로 밀어낸 영역(§13.1,
// AUTH_REQUIRED 절)을 우리가 채우는 부분이고, 이 토이의 본체.
package main

import "fmt"

// MaxDelegationDepth — 봇→봇 위임 최대 홉 수. 루트(0)=자기 일, 2까지 허용.
const MaxDelegationDepth = 2

// 스킬 분류
//
//	normal   : 위임 가능
//	sensitive: 위임 가능하되 소유주(사람) 승인 필요 → input-required
//	elevated : 위임 불가, 소유주 직접 실행만
type SkillClass int

const (
	SkillNormal SkillClass = iota
	SkillSensitive
	SkillElevated
)

type SkillDef struct {
	ID     string
	Owners []string // 서빙 tenant들 (Hermes 봇 또는 코딩에이전트 CLI)
	Class  SkillClass
}

func servedBy(def SkillDef, tenant string) bool {
	for _, o := range def.Owners {
		if o == tenant {
			return true
		}
	}
	return false
}

// Catalog — fleet 스킬 등록부. 코딩 스킬은 gjc/claude/codex가 함께 서빙:
//
//	code.review    읽기전용 → 위임 가능
//	code.implement 파일 수정 → sensitive(소유주 승인 필요)
var Catalog = []SkillDef{
	{ID: "logs.summarize", Owners: []string{"lumi"}, Class: SkillNormal},
	{ID: "dm.read", Owners: []string{"lumi"}, Class: SkillSensitive},
	{ID: "deploy.reset", Owners: []string{"yena"}, Class: SkillElevated},
	{ID: "weather.report", Owners: []string{"yena"}, Class: SkillNormal},
	{ID: "schedule.check", Owners: []string{"jena"}, Class: SkillNormal},
	{ID: "profile.digest", Owners: []string{"minami"}, Class: SkillNormal},
	{ID: "code.review", Owners: []string{"gjc", "claude", "codex"}, Class: SkillNormal},
	{ID: "code.implement", Owners: []string{"gjc", "claude", "codex"}, Class: SkillSensitive},
}

func skillDef(id string) (SkillDef, bool) {
	for _, s := range Catalog {
		if s.ID == id {
			return s, true
		}
	}
	return SkillDef{}, false
}

// PolicyDecision — 검문 결과.
type PolicyDecision struct {
	Verdict string // "allow" | "deny" | "ask_owner"
	Reason  string
}

// CheckDelegation — SendMessage 진입 시점의 정책 판정.
// 판정 순서: 카탈로그 존재 → elevated(위임 금지) → 권한 축소(토큰 grant) → depth → sensitive.
func CheckDelegation(claims *Claims, tenant, skill string) PolicyDecision {
	def, ok := skillDef(skill)
	if !ok {
		return PolicyDecision{"deny", fmt.Sprintf("unknown skill %q", skill)}
	}
	if !servedBy(def, tenant) {
		return PolicyDecision{"deny", fmt.Sprintf("skill %q is not served by tenant %q (served by %v)", skill, tenant, def.Owners)}
	}
	if def.Class == SkillElevated {
		return PolicyDecision{"deny", fmt.Sprintf("skill %q is elevated: never delegable, owner-executed only", skill)}
	}
	granted := false
	for _, s := range claims.Skills {
		if s == skill {
			granted = true
			break
		}
	}
	if !granted {
		return PolicyDecision{"deny", fmt.Sprintf("skill %q not in delegation grant of %q (attenuation: grants only shrink)", skill, claims.Sub)}
	}
	depth := len(claims.Act)
	if depth > MaxDelegationDepth {
		return PolicyDecision{"deny", fmt.Sprintf("delegation chain depth %d exceeds max %d (chain: %s)", depth, MaxDelegationDepth, chainString(claims))}
	}
	if def.Class == SkillSensitive {
		return PolicyDecision{"ask_owner", fmt.Sprintf("skill %q is sensitive: owner approval required before %q executes it on behalf of %s", skill, tenant, delegatorLabel(claims))}
	}
	return PolicyDecision{"allow", ""}
}

func chainString(c *Claims) string {
	s := c.Sub
	for i := len(c.Act) - 1; i >= 0; i-- {
		s += " ← " + c.Act[i].Sub
	}
	return s
}

func delegatorLabel(c *Claims) string {
	if len(c.Act) == 0 {
		return "itself"
	}
	return c.Act[0].Sub
}

// ExecuteSkill — 스킬 실행 시뮬레이션. 실 Hermes라면 여기서 프로필 봇 호출.
func ExecuteSkill(skill string, msg Message) string {
	switch skill {
	case "logs.summarize":
		return fmt.Sprintf("[lumi] last-24h log summary for %s (simulated)", textOf(msg))
	case "dm.read":
		return fmt.Sprintf("[lumi] DM digest for %s (simulated, owner-approved)", textOf(msg))
	case "weather.report":
		return "[yena] weather: Seoul 31°C clear (simulated)"
	case "schedule.check":
		return "[jena] schedule: 2 meetings tomorrow (simulated)"
	case "profile.digest":
		return "[minami] profile digest generated (simulated)"
	}
	return fmt.Sprintf("[%s] done (simulated)", skill)
}

func textOf(m Message) string {
	for _, p := range m.Parts {
		if p.Kind == "text" {
			return p.Text
		}
	}
	return ""
}

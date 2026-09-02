// bot.go — Hermes 봇 미니 서버. 실 Hermes 봇은 HTTP 서버가 아니라는 게 이 토이의
// 출발점이므로, 여기서는 봇마다 (a) 자기 카드 서빙 (b) 내부 시크릿 없는 JSON-RPC
// 호출 차단(bridge-only ingress)만 하는 최소 서버를 시뮬레이션한다.
package main

import (
	"crypto/subtle"
	"fmt"
	"net/http"
)

type Bot struct {
	ID           string
	bridgeSecret string // bridge 내부 호출 시크릿(값 검증용)
	mux          *http.ServeMux
}

// SetBridgeSecret — bridge 생성 후 내부 시크릿 주입.
func (b *Bot) SetBridgeSecret(sec string) { b.bridgeSecret = sec }

func NewBot(id string) *Bot {
	b := &Bot{ID: id, mux: http.NewServeMux()}
	b.mux.HandleFunc("GET /.well-known/agent-card.json", b.serveCard)
	b.mux.HandleFunc("POST /", b.serveGuarded)
	return b
}

func (b *Bot) card() AgentCard {
	return AgentCard{
		Name:               "hermes-bot-" + b.ID,
		Description:        fmt.Sprintf("Hermes profile bot %q behind bridge (direct JSON-RPC is rejected)", b.ID),
		Version:            "1.0.0",
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		AgentInterfaces:    []AgentInterface{{Name: b.ID, URL: "bridge:///", Tenant: b.ID}},
		Skills:             botSkills(b.ID),
	}
}

func botSkills(botID string) []Skill {
	out := []Skill{}
	for _, s := range Catalog {
		if servedBy(s, botID) {
			out = append(out, Skill{ID: s.ID, Name: s.ID})
		}
	}
	return out
}

func (b *Bot) serveCard(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, b.card())
}

// serveGuarded — 직접 호출(우회) 방어: X-Hermes-Bridge 내부 시크릿 값이
// 일치(상수시간 비교)하지 않으면 403. bridge→봇 호출에만 시크릿이 실린다.
func (b *Bot) serveGuarded(w http.ResponseWriter, r *http.Request) {
	sec := r.Header.Get("X-Hermes-Bridge")
	if sec == "" || b.bridgeSecret == "" ||
		subtle.ConstantTimeCompare([]byte(sec), []byte(b.bridgeSecret)) != 1 {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"error":  "direct agent-to-agent JSON-RPC is not accepted; ingress is bridge-only",
			"tenant": b.ID,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "tenant": b.ID, "note": "internal bridge→bot call"})
}

// Handler — 봇 서버 핸들러. 내부 시크릿 헤더(X-Hermes-Bridge)는 bridge→봇
// 호출 시 bridge 쪽에서 설정한다. 여기서는 검증만: 없으면 serveGuarded가 403.
func (b *Bot) Handler() http.Handler {
	return b.mux
}

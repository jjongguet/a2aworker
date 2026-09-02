// token.go — HMAC-SHA256 서명 토큰 + RFC 8693(토큰 교환)풍 위임 체인.
// OAuth 개념 시뮬레이션:
//   - 루트 토큰  = client_credentials 발급 (봇 자신 신원)
//   - 교환     = token exchange: 부모 토큰 → 축소된 자식 토큰 (skills 교집합, act 체인 누적)
package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Actor — 위임 체인의 한 고리 (RFC 8690 `act` 클레임 흉내).
type Actor struct {
	Sub    string   `json:"sub"`    // 위임한 주체
	Skills []string `json:"skills"` // 그 주체가 내려준 권한
}

// Claims — 토큰 페이로드.
type Claims struct {
	Sub     string   `json:"sub"`    // 현재 소지자 (호출자 봇)
	Act     []Actor  `json:"act"`    // 위임 체인 (원위임자 → ... → 직전 위임자)
	Skills  []string `json:"skills"` // 소지자가 남에게 요청할 수 있는 스킬
	Iat     int64    `json:"iat"`
	TraceID string   `json:"traceId"` // 체인 전체 공유 추적 ID
}

type token struct {
	header  string // base64url({"alg":"HS256","typ":"JOSE"}) — JWS 구조 흉내
	payload string
	sig     string
}

func now() int64 { return time.Now().Unix() }

var b64 = base64.RawURLEncoding

func newSigningKey() []byte {
	k := make([]byte, 32)
	rand.Read(k)
	return k
}

func sign(key []byte, payload []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	return b64.EncodeToString(mac.Sum(nil))
}

// mint — 클레임으로 토큰 생성.
func mint(key []byte, c Claims) string {
	payload, _ := json.Marshal(c)
	p := b64.EncodeToString(payload)
	return fmt.Sprintf("%s.%s.%s", b64.EncodeToString([]byte(`{"alg":"HS256","typ":"JOSE"}`)), p, sign(key, payload))
}

// verify — 서명 검증 후 클레임 반환. 위조/변조 시 에러.
func verify(key []byte, tok string) (*Claims, error) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("malformed token")
	}
	payload, err := b64.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("payload decode: %w", err)
	}
	if !hmac.Equal([]byte(sign(key, payload)), []byte(parts[2])) {
		return nil, fmt.Errorf("signature mismatch")
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return nil, fmt.Errorf("claims decode: %w", err)
	}
	return &c, nil
}

// exchange — RFC 8693 토큰 교환: 부모(신원 증명됨) → 자식(위임, 권한 축소).
// 요청 스킬이 부모 스킬의 부분집합이어야 하고, act 체인에 부모가 한 칸 쌓인다.
func exchange(key []byte, parent *Claims, childSub string, requestedSkills []string) (string, *Claims, error) {
	granted := intersect(parent.Skills, requestedSkills)
	if len(requestedSkills) > 0 && len(granted) == 0 {
		return "", nil, fmt.Errorf("token exchange refused: no overlap between parent grant and requested skills")
	}
	act := append([]Actor{}, parent.Act...)
	act = append(act, Actor{Sub: parent.Sub, Skills: parent.Skills})
	child := &Claims{
		Sub:     childSub,
		Act:     act,
		Skills:  granted,
		Iat:     time.Now().Unix(),
		TraceID: parent.TraceID,
	}
	if child.TraceID == "" {
		child.TraceID = fmt.Sprintf("trace-%x", time.Now().UnixNano())
	}
	return mint(key, *child), child, nil
}

func intersect(a, b []string) []string {
	set := map[string]bool{}
	for _, s := range a {
		set[s] = true
	}
	var out []string
	for _, s := range b {
		if set[s] {
			out = append(out, s)
		}
	}
	return out
}

func newTraceID() string {
	return fmt.Sprintf("trace-%x", time.Now().UnixNano())
}

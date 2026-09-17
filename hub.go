// hub.go — 중앙 허브 모드. "SSH 없이 남의 PC 참여"의 정답:
// 워커는 아웃바운드 폴링만 하고(GitHub Actions self-hosted runner 선례), 허브는
// 초대코드 발급 → worker 등록 → 작업 큐 → 배달 → 결과 수집을 담당한다.
//
//	워커 ──join(invite)──▶ 허브: 1회용 초대코드를 workerID+worker 토큰으로 교환
//	워커 ◀─poll(job)───── 허브: 대기 중 작업 배달 (long-poll)
//	워커 ──result────────▶ 허브: 실행 결과 반영 → 호출자 GetTask로 회수
//
// 보안: 초대코드는 1회용·TTL 있음. worker 토큰은 HMAC(bridge와 같은 구조).
package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---- 데이터 모델 ----

type Invite struct {
	Code      string   // 1회용 (예: a2ainv-xxxx)
	WorkerID  string   // 초대될 워커 이름
	Skills    []string // 이 워커에 부여할 스킬 grant
	ExpiresAt int64
	Used      bool
}

type WorkerReg struct {
	ID         string
	TokenHash  string // HMAC-SHA256(token) — 원문 미보관
	Skills     []string
	JoinedAt   int64
	LastPoll   int64
	PendingJob *HubJob // 배달 대기 작업(선입선출 단일 슬롯 — 토이 스코프)
}

type HubJob struct {
	ID        string
	Tenant    string // 워커 측 tenant (보통 workerID와 동일)
	Skill     string
	Prompt    string
	Metadata  map[string]any
	CreatedAt int64
}

type HubResult struct {
	State    string // completed | failed
	Text     string
	StoredAt int64 // resultTTL 경과 후 GC 대상
}

// Hub — 상태 전체.
type Hub struct {
	mu        sync.Mutex
	key       []byte // worker 토큰 서명 키
	invites   map[string]*Invite
	workers   map[string]*WorkerReg
	jobs      map[string]*HubJob        // 진행 중/대기 작업
	results   map[string]*HubResult     // jobID → 결과
	waiters   map[string][]chan *HubJob // workerID → long-poll 대기자
	nextID    int
	resultTTL time.Duration // 완료 결과 보관 기한(이후 자동 삭제 — 누수 방지)
	maxRetry  int           // 유실 작업 최대 재배달 횟수
	retried   map[string]int
}

func NewHub() *Hub {
	k := make([]byte, 32)
	rand.Read(k)
	return &Hub{
		key:       k,
		invites:   map[string]*Invite{},
		workers:   map[string]*WorkerReg{},
		jobs:      map[string]*HubJob{},
		results:   map[string]*HubResult{},
		waiters:   map[string][]chan *HubJob{},
		retried:   map[string]int{},
		resultTTL: 1 * time.Hour,
		maxRetry:  2,
	}
}

// StartGC — 결과 TTL 정리 + 오래된 초대코드 청소. runHub에서 백그라운드로 구동.
func (h *Hub) StartGC(every time.Duration) {
	go func() {
		for range time.Tick(every) {
			h.mu.Lock()
			cutoff := now() - int64(h.resultTTL.Seconds())
			for id, res := range h.results {
				if res.StoredAt < cutoff {
					delete(h.results, id)
				}
			}
			for code, inv := range h.invites {
				if inv.Used || now() > inv.ExpiresAt {
					delete(h.invites, code)
				}
			}
			h.mu.Unlock()
			// RedeliverStuck이 자체 잠금을 잡으므로 unlock 후 호출.
			h.RedeliverStuck(2 * time.Minute) // 2분 넘게 결과 없는 작업 재배일
		}
	}()
}

// ---- 초대코드 ----

// MintInvite — 운영자가 초대코드 생성. TTL 기본 24h.
func (h *Hub) MintInvite(workerID string, skills []string, ttl time.Duration) (*Invite, error) {
	if workerID == "" || strings.ContainsAny(workerID, " \t/") {
		return nil, fmt.Errorf("invalid worker id %q", workerID)
	}
	buf := make([]byte, 16)
	rand.Read(buf)
	code := "a2ainv-" + hex.EncodeToString(buf)
	h.mu.Lock()
	defer h.mu.Unlock()
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	inv := &Invite{Code: code, WorkerID: workerID, Skills: skills, ExpiresAt: now() + int64(ttl.Seconds())}
	h.invites[code] = inv
	return inv, nil
}

// ---- worker 토큰 (HMAC, token.go와 동일 형식) ----

func (h *Hub) hashToken(tok string) string {
	sum := sign(h.key, []byte("hash:"+tok))
	return sum
}

func (h *Hub) mintWorkerToken(workerID string, skills []string) string {
	return mint(h.key, Claims{Sub: "worker:" + workerID, Skills: skills, Iat: now(), TraceID: newTraceID()})
}

// verifyWorker — Bearer에서 claims 복원 + 등록부 일치 확인. 등록부의 TokenHash와
// 상수시간 비교까지 통과해야 유효하다: 서명만 보고 지나가면 같은 workerID의 옛
// 토큰이 re-join 이후에도 영구 유효해 폐기(revocation)가 없어진다.
func (h *Hub) verifyWorker(bearerToken string) (*WorkerReg, bool) {
	claims, err := verify(h.key, bearerToken)
	if err != nil || !strings.HasPrefix(claims.Sub, "worker:") {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	w, ok := h.workers[strings.TrimPrefix(claims.Sub, "worker:")]
	if !ok || subtle.ConstantTimeCompare([]byte(h.hashToken(bearerToken)), []byte(w.TokenHash)) != 1 {
		return nil, false
	}
	return w, true
}

// ---- 운영자(admin) 라우팅 ----

// Handler — 허브 HTTP 전체 라우팅. worker 면(join/poll/result POST)은 각자의
// 인증(초대코드·worker 토큰)을 쓰고, 운영자 면(invite/dispatch/result/<id>/
// workers)은 adminToken Bearer(상수시간 비교)로 보호한다. adminToken이 비면
// 운영자 면은 전부 403으로 폐쇄(fail-closed) — 무인증 dispatch는 원격 워커에서
// 임의 프롬프트 실행(RCE)이므로 기본 개방하지 않는다.
func (h *Hub) Handler(adminToken string) http.Handler {
	mux := http.NewServeMux()
	requireAdmin := func(w http.ResponseWriter, r *http.Request) bool {
		if adminToken == "" {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "hub admin API disabled — set A2A_ADMIN_TOKEN on the hub"})
			return false
		}
		presented := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(presented), []byte(adminToken)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "admin auth failed"})
			return false
		}
		return true
	}
	mux.HandleFunc("/hub/join", h.serveJoin)
	mux.HandleFunc("/hub/poll", h.servePoll)
	mux.HandleFunc("/hub/result", h.serveResult)
	mux.HandleFunc("/hub/invite", func(w http.ResponseWriter, r *http.Request) {
		if !requireAdmin(w, r) {
			return
		}
		var req struct {
			WorkerID string   `json:"workerId"`
			Skills   []string `json:"skills"`
			TTLH     int      `json:"ttlHours,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.WorkerID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad invite request"})
			return
		}
		inv, err := h.MintInvite(req.WorkerID, req.Skills, time.Duration(req.TTLH)*time.Hour)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"code": inv.Code, "workerId": inv.WorkerID, "skills": inv.Skills,
			"expiresAt": inv.ExpiresAt,
			"joinHint":  fmt.Sprintf("a2aworker join --hub <this-hub-url> --invite %s", inv.Code),
		})
	})
	mux.HandleFunc("/hub/dispatch", func(w http.ResponseWriter, r *http.Request) {
		if !requireAdmin(w, r) {
			return
		}
		var req struct {
			Tenant   string         `json:"tenant"`
			Skill    string         `json:"skill"`
			Prompt   string         `json:"prompt"`
			Metadata map[string]any `json:"metadata,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Tenant == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad dispatch"})
			return
		}
		id, err := h.Dispatch(req.Tenant, req.Skill, req.Prompt, req.Metadata)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"jobId": id})
	})
	mux.HandleFunc("/hub/result/", func(w http.ResponseWriter, r *http.Request) {
		if !requireAdmin(w, r) {
			return
		}
		jobID := strings.TrimPrefix(r.URL.Path, "/hub/result/")
		res, ok := h.ResultOf(jobID)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "no result yet"})
			return
		}
		parts := strings.SplitN(res, "|", 2)
		writeJSON(w, http.StatusOK, map[string]any{"state": parts[0], "output": parts[1]})
	})
	mux.HandleFunc("/hub/workers", func(w http.ResponseWriter, r *http.Request) {
		if !requireAdmin(w, r) {
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"workers": h.WorkersSnapshot()})
	})
	return mux
}

// ---- HTTP 핸들러 ----

type joinReq struct {
	Code string   `json:"code"`
	Host string   `json:"host,omitempty"` // 워커 자기소개(표시용)
	ACPs []string `json:"acps,omitempty"` // 로컬 ACP 에이전트 목록(표시용)
}

func (h *Hub) serveJoin(w http.ResponseWriter, r *http.Request) {
	var req joinReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, RPCResponse{Error: &RPCError{Code: CodeParseError, Message: "bad join request"}})
		return
	}
	h.mu.Lock()
	inv, ok := h.invites[req.Code]
	if !ok {
		h.mu.Unlock()
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "unknown invite code"})
		return
	}
	if inv.Used || now() > inv.ExpiresAt {
		delete(h.invites, req.Code)
		h.mu.Unlock()
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "invite expired or already used"})
		return
	}
	inv.Used = true
	tok := h.mintWorkerToken(inv.WorkerID, inv.Skills)
	h.workers[inv.WorkerID] = &WorkerReg{
		ID: inv.WorkerID, TokenHash: h.hashToken(tok), Skills: inv.Skills, JoinedAt: now(), LastPoll: now(),
	}
	h.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"token": tok, "workerId": inv.WorkerID, "skills": inv.Skills,
	})
}

// pollReq — long-poll: workerID는 URL 경로, Bearer로 인증.
func (h *Hub) servePoll(w http.ResponseWriter, r *http.Request) {
	wreg, ok := h.verifyWorker(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "worker auth failed"})
		return
	}
	ch := make(chan *HubJob, 1)
	timeout := time.After(25 * time.Second)

	h.mu.Lock()
	h.workers[wreg.ID].LastPoll = now()
	// 대기 중 작업 즉시 배달
	if j := h.workers[wreg.ID].PendingJob; j != nil {
		h.workers[wreg.ID].PendingJob = nil
		h.retried[j.ID]++ // 배달 시도 횟수 기록 — 결과 미보고 시 재배달 판단에 사용
		h.mu.Unlock()
		writeJSON(w, http.StatusOK, j)
		return
	}
	h.waiters[wreg.ID] = append(h.waiters[wreg.ID], ch)
	// 연결 끊김 대응: 컨텍스트 종료 시 대기열에서 자기 채널 제거(누수 방지).
	go func() {
		<-r.Context().Done()
		h.mu.Lock()
		defer h.mu.Unlock()
		chs := h.waiters[wreg.ID]
		for i, c := range chs {
			if c == ch {
				h.waiters[wreg.ID] = append(chs[:i], chs[i+1:]...)
				break
			}
		}
		if len(h.waiters[wreg.ID]) == 0 {
			delete(h.waiters, wreg.ID)
		}
	}()
	h.mu.Unlock()

	select {
	case j := <-ch:
		writeJSON(w, http.StatusOK, j)
	case <-timeout:
		writeJSON(w, http.StatusOK, map[string]any{"none": true}) // 할 일 없음
	case <-r.Context().Done():
		// 연결 끊김 — 대기열에서 제거
	}
}

// resultReq — 워커가 실행 결과 보고.
type resultReq struct {
	JobID  string `json:"jobId"`
	State  string `json:"state"` // completed | failed
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

func (h *Hub) serveResult(w http.ResponseWriter, r *http.Request) {
	wreg, ok := h.verifyWorker(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "worker auth failed"})
		return
	}
	var req resultReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, RPCResponse{Error: &RPCError{Code: CodeParseError, Message: "bad result"}})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	// verifyWorker가 돌려준 포인터는 락 밖에서 오래됐을 수 있다(re-join 교체).
	// 락 안에서 현재 등록부를 다시 가져온다.
	if wreg = h.workers[wreg.ID]; wreg == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "worker no longer registered"})
		return
	}
	job, ok := h.jobs[req.JobID]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown job"})
		return
	}
	// 소유 검증: 이 job이 실제로 이 워커에게 배달된 것인지.
	if job.Tenant != wreg.ID {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "job belongs to another worker"})
		return
	}
	state := req.State
	text := req.Output
	if state != StateCompleted {
		state = StateFailed
		text = req.Error
	}
	h.results[req.JobID] = &HubResult{State: state, Text: text, StoredAt: now()}
	// 완료된 작업은 큐에서 제거(결과는 resultTTL 동안 보관 후 GC).
	delete(h.jobs, req.JobID)
	// 재배일 레이스 방지: 실행 지연으로 RedeliverStuck이 이 잡을 PendingJob에
	// 재진열해 둔 상태면, 결과 확정 뒤 다음 poll에서 같은 잡이 다시 배달되지
	// 않게 치운다(2026-08-26 종단 검증에서 15분 잡이 두 번 실행됐던 원인).
	if wreg.PendingJob != nil && wreg.PendingJob.ID == req.JobID {
		wreg.PendingJob = nil
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// Dispatch — 허브 API: 외부 호출자(브리지 remote executor 또는 curl)가 작업 접수.
// tenant=워커ID 매핑: 카탈로그의 tenant가 곧 workerID다.
func (h *Hub) Dispatch(tenant, skill, prompt string, metadata map[string]any) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	wreg, ok := h.workers[tenant]
	if !ok {
		return "", fmt.Errorf("no worker joined for tenant %q", tenant)
	}
	h.nextID++
	id := fmt.Sprintf("job-%04d", h.nextID)
	job := &HubJob{ID: id, Tenant: tenant, Skill: skill, Prompt: prompt, Metadata: metadata, CreatedAt: now()}
	h.jobs[id] = job
	if chs := h.waiters[tenant]; len(chs) > 0 {
		ch := chs[0]
		h.waiters[tenant] = chs[1:]
		ch <- job
	} else {
		wreg.PendingJob = job
	}
	return id, nil
}

// RedeliverStuck — 유실 작업 재배일: 배달됐지만 결과가 안 온 작업을 다시
// pending으로 되돌린다(maxRetry 초과 시 폐기). StartGC 루프에서 호출.
func (h *Hub) RedeliverStuck(olderThan time.Duration) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	cutoff := now() - int64(olderThan.Seconds())
	for id, job := range h.jobs {
		if h.retried[id] >= h.maxRetry {
			delete(h.jobs, id)
			delete(h.retried, id)
			continue
		}
		wreg := h.workers[job.Tenant]
		if wreg == nil || wreg.PendingJob != nil {
			continue // 워커 부재 또는 이미 대기 중인 작업 — 건너뜀
		}
		if job.CreatedAt <= cutoff && h.results[id] == nil {
			wreg.PendingJob = job
			n++
		}
	}
	return n
}

// ResultOf — job 결과 폴링 (호출자 측).
func (h *Hub) ResultOf(jobID string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	res, ok := h.results[jobID]
	if !ok {
		return "", false
	}
	return res.State + "|" + res.Text, true
}

// WorkersSnapshot — 카드/상태 출력용.
func (h *Hub) WorkersSnapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := []string{}
	for id, w := range h.workers {
		sort.Strings(w.Skills)
		out = append(out, fmt.Sprintf("%s skills=%v joined=%d pending=%v", id, w.Skills, w.JoinedAt, w.PendingJob != nil))
	}
	sort.Strings(out)
	return out
}

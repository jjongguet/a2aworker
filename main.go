// main.go — 데모 드라이버: bridge :9600 + 봇 4기(:9601~04) 기동 후
// 시나리오 1(정상 위임 체인)을 실제 HTTP로 주고받고 종료.
// 전체 시나리오 검증은 go test (-v)가 담당.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	if mainModes() {
		return
	}
}

// mainModes — 서브커맨드 분기: hub / join / work (기본은 기존 bridge 데모).
func mainModes() bool {
	switch os.Getenv("A2A_MODE") {
	case "", "bridge":
		return false // 기존 경로 유지
	case "hub":
		runHub()
		return true
	case "join":
		args := os.Args[1:]
		hub, invite := "", ""
		for i := 0; i < len(args); i++ {
			switch args[i] {
			case "--hub":
				i++
				if i < len(args) {
					hub = args[i]
				}
			case "--invite":
				i++
				if i < len(args) {
					invite = args[i]
				}
			}
		}
		if hub == "" || invite == "" {
			fmt.Println("usage: A2A_MODE=join a2aworker --hub <url> --invite <code>")
			os.Exit(2)
		}
		if err := runJoin(hub, invite); err != nil {
			fmt.Fprintln(os.Stderr, "join failed:", err)
			os.Exit(1)
		}
		return true
	case "work":
		acp := os.Getenv("A2A_ACP_BIN")
		if acp == "" {
			acp = "gjc acp"
		}
		interval := 5 * time.Second
		if err := runWork(os.Getenv("A2A_HUB"), acp, interval); err != nil {
			fmt.Fprintln(os.Stderr, "work failed:", err)
			os.Exit(1)
		}
		return true
	}
	return false
}

// splitACP — "gjc acp" 같은 문자열을 argv로.
func splitACP(s string) []string { return strings.Fields(s) }

// runHub — 허브 모드: :9600에 hub HTTP 서버 + 초대코드 콘솔 명령 안내.
func runHub() {
	adminToken := os.Getenv("A2A_ADMIN_TOKEN")
	if adminToken == "" {
		fmt.Fprintln(os.Stderr, "warning: A2A_ADMIN_TOKEN not set — operator endpoints (/hub/invite, /hub/dispatch, /hub/result/<id>, /hub/workers) are DISABLED (403).")
	}
	hub := NewHub()
	hub.StartGC(5 * time.Minute)
	fmt.Println("hub mode: :9600 (endpoints /hub/join|poll|result|invite|dispatch|result/<id>|workers)")
	fmt.Println("운영자 흐름:")
	fmt.Println("  1. 초대코드 발급: curl -X POST localhost:9600/hub/invite -H 'Authorization: Bearer $A2A_ADMIN_TOKEN' -d '{\"workerId\":\"gjc\",\"skills\":[\"code.review\",\"code.implement\"]}'")
	fmt.Println("  2. 남의 PC에서: a2aworker join --hub http://<허브>:9600 --invite <코드>")
	fmt.Println("     (또는 A2A_MODE=join a2aworker --hub ... --invite ...)")
	fmt.Println("  3. 워커 상주: A2A_MODE=work a2aworker")
	if err := http.ListenAndServe(":9600", hub.Handler(adminToken)); err != nil {
		fmt.Fprintln(os.Stderr, "hub server failed:", err)
		os.Exit(1)
	}
}

func runBridgeDemo() {
	bots := map[string]*Bot{
		"jena":   NewBot("jena"),
		"lumi":   NewBot("lumi"),
		"yena":   NewBot("yena"),
		"minami": NewBot("minami"),
		"gjc":    NewBot("gjc"),
		"claude": NewBot("claude"),
		"codex":  NewBot("codex"),
	}
	bridge := NewBridge("http://localhost:9600", bots)
	for _, b := range bots {
		b.SetBridgeSecret(string(bridge.InternalSecret()))
	}

	// 실행기 선택: 기본 sim(실 fleet 무접촉). A2A_EXECUTOR=hermes|remote 로 교체.
	// 실전 활성화는 배포 시에만(연동 준비 상태에서는 키지 않는다).
	switch os.Getenv("A2A_EXECUTOR") {
	case "hermes":
		he := NewHermesExecutor()
		if v := os.Getenv("HERMES_BIN"); v != "" {
			he.Bin = v
		}
		if v := os.Getenv("HERMES_BUDGET_USD"); v != "" {
			he.MaxBudget = v
		}
		if v := os.Getenv("HERMES_HOME_DIR"); v != "" {
			he.HomeDir = v
		}
		if v := os.Getenv("HERMES_TIMEOUT_SECS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				he.Timeout = time.Duration(n) * time.Second
			}
		}
		bridge.SetExecutor(he)
		fmt.Printf("executor: hermes (bin=%s budget=%q home=%q timeout=%s)\n", he.Bin, he.MaxBudget, he.HomeDir, he.Timeout)
	case "cli":
		// 코딩에이전트 provider 모드: gjc/claude/codex를 tenant로 서빙.
		// A2A_CMD_<TENANT>="..."(공백 분리)로 템플릿 오버라이드 가능.
		ce := NewCLIExecutor()
		for _, tenant := range []string{"gjc", "claude", "codex"} {
			if v := os.Getenv("A2A_CMD_" + strings.ToUpper(tenant)); v != "" {
				ce.Cmds[tenant] = strings.Fields(v)
			}
		}
		bridge.SetExecutor(ce)
		fmt.Printf("executor: cli (tenants=%d timeout=%s)\n", len(ce.Cmds), ce.Timeout)
	case "remote":
		// 크로스호스트 federation: A2A_REMOTE_<TENANT>=URL 라우트(예:
		// A2A_REMOTE_lumi=http://host:9600)로 원격 브리지에 실행 위임.
		// 원격 인바운드는 상대가 A2A_FEDERATION_INBOUND_TOKEN로 수락하도록 배포.
		re := &RemoteExecutor{
			Routes:   remoteRoutesFromEnv(),
			FedToken: os.Getenv("A2A_FEDERATION_TOKEN"),
		}
		// A2A_REMOTE_TIMEOUT_SECS: 원격 실행 예산(초). 기본 5초는 sim/빠른 스킬용이고,
		// hermes 같은 LLM 실행기를 원격으로 부르면 180~300초로 늘려야 한다.
		if v := os.Getenv("A2A_REMOTE_TIMEOUT_SECS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				re.Timeout = time.Duration(n) * time.Second
			}
		}
		bridge.SetExecutor(re)
		bridge.SetFedClient(re.clientFor)
		fmt.Printf("executor: remote federation (caller=%s routes=%d)\n", re.caller(), len(re.Routes))
		for _, tenant := range sortedKeys(re.Routes) {
			fmt.Printf("  route %-10s → %s\n", tenant, re.Routes[tenant])
		}
	default:
		fmt.Println("executor: sim (set A2A_EXECUTOR=hermes for live, =remote for federation)")
	}

	go http.ListenAndServe(":9600", bridge.Handler())
	for i, id := range []string{"jena", "lumi", "yena", "minami", "gjc", "claude", "codex"} {
		go http.ListenAndServe(fmt.Sprintf(":960%d", i+1), bots[id].Handler())
	}
	time.Sleep(200 * time.Millisecond)

	// --- 시나리오 1 데모: jena → lumi (logs.summarize) ---
	jenaTok := bridge.MintClientToken("jena", []string{"logs.summarize", "dm.read"})

	card := get("http://localhost:9600/.well-known/agent-card.json")
	fmt.Println("── bridge AgentCard (tenants) ──")
	var c AgentCard
	json.Unmarshal(card, &c)
	for _, i := range c.AgentInterfaces {
		fmt.Printf("  interface %-8s tenant=%s url=%s\n", i.Name, i.Tenant, i.URL)
	}

	// A2A_SERVE=1이면 데모를 건너뛰고 서버를 무기한 상주(수동 스모크용).
	if os.Getenv("A2A_SERVE") == "1" {
		fmt.Println("serve mode: holding (demo skipped)")
		// 스모크용 루트 토큰 1개 출력(스킬: code.review/code.implement/logs.summarize).
		root := bridge.MintClientToken("smoke", []string{"code.review", "code.implement", "logs.summarize"})
		fmt.Printf("ROOT_TOKEN=%s\n", root)
		// stdout 외에 파일로도 기록(bg 잡 stdout 회수 전 수동 스모크용).
		if v := os.Getenv("A2A_TOKEN_FILE"); v != "" {
			os.WriteFile(v, []byte(root), 0o600)
		}
		select {}
	}

	fmt.Println("\n── SendMessage tenant=lumi skill=logs.summarize ──")
	body := map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "SendMessage",
		"params": MessageSendParams{
			Tenant:   "lumi",
			Message:  Message{Role: "agent", Parts: []Part{TextPart("summarize yesterday's fleet logs")}},
			Metadata: map[string]any{"skill": "logs.summarize"},
		},
	}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "http://localhost:9600/", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+jenaTok)
	req.Header.Set("A2A-Version", "1.0")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	var rpc RPCResponse
	json.NewDecoder(resp.Body).Decode(&rpc)
	taskJSON, _ := json.MarshalIndent(rpc.Result, "", "  ")
	fmt.Println(string(taskJSON))

	var t Task
	b, _ := json.Marshal(rpc.Result)
	json.Unmarshal(b, &t)
	fmt.Printf("\nverdict: state=%s · traceId present=%v\n", t.Status.State, t.Metadata["delegation"] != nil)
}

func get(url string) []byte {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return buf
}

// serveHold — A2A_SERVE=1일 때 데모를 건너뛰고 서버를 무기한 상주시킨다
// (수동 스모크: curl로 SendMessage/GetTask 직접 호출).
func serveHold() {
	select {}
}

// remoteRoutesFromEnv — A2A_REMOTE_<TENANT>=URL 환경변수를 federation 라우트로
// 파싱 (예: A2A_REMOTE_lumi=http://host:9600 → Routes["lumi"]).
func remoteRoutesFromEnv() map[string]string {
	routes := map[string]string{}
	for _, kv := range os.Environ() {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(k, "A2A_REMOTE_") {
			continue
		}
		tenant := strings.ToLower(strings.TrimPrefix(k, "A2A_REMOTE_"))
		if tenant != "" && v != "" {
			routes[tenant] = v
		}
	}
	return routes
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

# a2aworker

A2A(에이전트 간 위임) 브리지·허브·워커 실험 구현 (Go, 외부 의존성 없음).

- **bridge** — 단일 A2A JSON-RPC 엔드포인트(:9600). 정책 게이트웨이: HMAC 토큰 검증 → tenant 라우팅 → 위임 정책 판정(`policy.go`) → Task 상태머신.
- **hub** — 중앙 허브(:9600). NAT 뒤 워커를 아웃바운드 long-poll만으로 참여시키는 작업 큐 (GitHub Actions self-hosted runner 선례).
- **worker** — "남의 PC" 측 에이전트. join(초대코드→토큰 교환) → long-poll로 잡 수령 → 로컬 ACP 에이전트(`gjc acp` 등 stdio) 실행 → 결과 보고.

## 빌드·테스트

```sh
go build ./...
go test ./... -race
```

## 실행 모드

`A2A_MODE` 환경변수 (기본값 없음 = bridge 데모 드라이버 1회 실행 후 종료):

| 모드 | 명령 | 역할 |
|---|---|---|
| `hub` | `A2A_MODE=hub go run .` | 중앙 허브 서버 (:9600) |
| `join` | `A2A_MODE=join a2aworker --hub <url> --invite <코드>` | 초대코드를 워커 토큰으로 교환, `~/.a2aworker/worker.json` 저장 |
| `work` | `A2A_MODE=work a2aworker` | long-poll 상주 (ACP 실행기: `A2A_ACP_BIN`, 기본 `gjc acp`) |
| `bridge` | `A2A_MODE=bridge go run .` | bridge 데모 (실행기 선택: `A2A_EXECUTOR=sim\|hermes\|cli\|remote`) |

## 허브 보안 모델 (2026-09-17 강화)

엔드포인트는 **운영자 면**과 **워커 면**으로 나뉜다.

| 경로 | 면 | 인증 |
|---|---|---|
| `POST /hub/invite` | 운영자 | `A2A_ADMIN_TOKEN` Bearer |
| `POST /hub/dispatch` | 운영자 | `A2A_ADMIN_TOKEN` Bearer |
| `GET /hub/result/<jobId>` | 운영자 | `A2A_ADMIN_TOKEN` Bearer |
| `GET /hub/workers` | 운영자 | `A2A_ADMIN_TOKEN` Bearer |
| `POST /hub/join` | 워커 | 1회용 초대코드 (TTL 기본 24h) |
| `GET /hub/poll` | 워커 | 워커 토큰 Bearer |
| `POST /hub/result` | 워커 | 워커 토큰 Bearer |

규칙:

- **`A2A_ADMIN_TOKEN` 미설정 시 운영자 면은 전부 403으로 폐쇄된다 (fail-closed).** 무인증 `dispatch`는 워커가 `GJC_ACP_PERMISSION_MODE=always-allow`로 임의 프롬프트를 실행하는 **RCE**가 되므로 기본 개방하지 않는다.
- 워커 토큰은 허브 서명키로 HMAC 발급되며 등록부의 `TokenHash`와 상수시간 비교된다. **같은 workerID가 재가입하면 옛 토큰은 즉시 폐기**된다.
- 허브 재시작 시 서명키가 재생성되므로 기존 워커 토큰은 전부 무효 — 워커는 401을 치명 에러로 종료하고 재가입해야 한다.

## 운영자 흐름

```sh
# 1) 허브 기동
A2A_ADMIN_TOKEN=<시크릿> A2A_MODE=hub go run .

# 2) 초대코드 발급
curl -X POST localhost:9600/hub/invite \
  -H "Authorization: Bearer $A2A_ADMIN_TOKEN" \
  -d '{"workerId":"gjc","skills":["code.review","code.implement"]}'

# 3) 남의 PC에서 (한 번)
a2aworker join --hub http://<허브>:9600 --invite <코드>
A2A_MODE=work a2aworker   # 상주

# 4) 작업 접수 → 결과 회수
curl -X POST localhost:9600/hub/dispatch \
  -H "Authorization: Bearer $A2A_ADMIN_TOKEN" \
  -d '{"tenant":"gjc","skill":"code.review","prompt":"..."}'
curl localhost:9600/hub/result/<jobId> -H "Authorization: Bearer $A2A_ADMIN_TOKEN"
```

## 신뢰성 계약

- **결과 보고 재시도**: 워커는 결과 보고를 백오프(1s→2s→4s→8s)로 재시도한다. 최종 실패 시 로그로 경고 — 허브는 미보고 잡을 재배일하므로 부수효과 있는 프롬프트의 중복 실행이 가능하다.
- **재배일**: 결과 없는 잡은 2분 후 재배열, 최대 2회. 결과 보고가 재진열된 PendingJob을 치운다(이중 실행 방지 회귀 테스트 있음: `TestHubResultClearsRedeliveredPending`).
- **완료 결과는 1시간 보관 후 GC.**

## bridge 실행기 (`A2A_EXECUTOR`)

- `sim` (기본) — 시뮬레이션. 실 fleet 무접촉.
- `cli` — `gjc`/`claude`/`codex` tenant를 로컬 CLI로 서빙 (`A2A_CMD_<TENANT>` 오버라이드).
- `remote` — 크로스호스트 federation. `A2A_REMOTE_<TENANT>=URL` 라우트 + `A2A_FEDERATION_TOKEN`. 원격측은 `A2A_FEDERATION_INBOUND_TOKEN`로 인바운드 수용.
- `hermes` — hermes 서비스 유닛(`deploy/hermes-a2a-bridge.service`) 참조.

## 문서 규칙

행동(엔드포인트·인증·에러 계약)을 바꾸는 커밋은 이 README의 해당 표/섹션을 같이 수정한다.

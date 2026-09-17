# a2aworker — 에이전트 맵

A2A 위임 브리지·허브·워커 실험 (Go, 외부 의존성 없음).

> **포지션 (2026-09-17 판정): 소비자 없는 실험 보관.**
> 운영 부담 0(보안 패치·문서 정리 완료). "남의 PC에 에이전트 위임" 패턴의 씨앗으로 보관하며, 워크스페이스 정리 시 삭제 1순위다. 소비자가 생기면 이 판정을 갱신한다.

**진실 소재: `README.md`** — 보안 모델(A2A_ADMIN_TOKEN fail-closed, 워커 토큰 폐기), 엔드포인트 인증 표, 신뢰성 계약(재시도·재배일)이 전부 거기 있다. 이 파일은 지도만 제공한다.

## 파일 맵

```
main.go              모드 분기 (A2A_MODE=hub|join|work|bridge) + 데모 드라이버
hub.go               허브: Handler(adminToken) 라우팅, join/poll/result, Dispatch, 재배일
worker.go            워커: long-poll 루프, ACP stdio 실행(runACPAgent), 보고 재시도
bridge.go            A2A JSON-RPC 브리지: 정책 게이트웨이, Task 상태머신, owner decision 중계
token.go             HMAC 토큰 + 위임 체인 (RFC 8693풍 exchange)
policy.go            위임 정책 판정
remote_executor.go   크로스호스트 federation 실행기
a2aclient.go         A2A 호출측 클라이언트
executor.go|cli_executor.go   실행기 (sim/hermes/cli)
a2a.go|bot.go        스키마·봇
deploy/              hermes 서비스 유닛
```

## 빌드·테스트

```sh
go build ./...
go test ./... -race
```

## 규칙

- 엔드포인트·인증·에러 계약을 바꾸는 커밋은 README의 해당 표를 같이 수정한다 (README "문서 규칙" 참조).
- 보안 관련 변경은 회귀 테스트를 수반한다 (기존: TestHubOperatorEndpointsRequireAdmin, TestHubRejoinRevokesOldToken).

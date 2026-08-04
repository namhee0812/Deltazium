# deploy — 베어메탈 인프라 (SQueryDev 서버)

2026-07-23 결정: docker-compose 대신 베어메탈 기동을 1순위로 한다. compose 포장은 추후.

## 구성

| 컴포넌트 | 버전 | 포트 | 비고 |
|---|---|---|---|
| PostgreSQL | 15.3 (`/home/dstream/dshome/postgresql-15.3` 바이너리 재사용) | 5433 | DB: `deltazium`(메타데이터), `iceberg_catalog`(Iceberg JDBC 카탈로그) |
| MinIO | RELEASE.2025-09-07 | 9010 (console 9011) | 버킷 `deltazium-warehouse`. squery 계정의 MinIO(9000)와 별개 — 복구 리허설이 파괴적이라 격리 |
| Kafka | 4.3.1 (KRaft 단일 노드) | 9092 (controller 9093) | retention 24h — Kafka는 짧은 버퍼 |
| Kafka Connect | Kafka 4.3.1 동봉, distributed 단일 워커 | 8083 | JSON converter, schemas.enabled=true |

Connect 플러그인 (`~/deltazium-runtime/connect-plugins/`):
- Debezium Oracle source 3.6.0.Final (ojdbc11 동봉)
- Debezium JDBC sink 3.6.0.Final
- Apache Iceberg sink 1.11.0 (`apache-iceberg-1.11.0` 태그를 소스 빌드 — 공식 프리빌드 배포판 없음. 빌드 산출물 버전 표기는 1.12.0-SNAPSHOT이지만 소스는 1.11.0)

## 디렉터리

- repo `deploy/` — 스크립트와 설정 템플릿만 (버전 관리 대상)
- `~/deltazium-runtime/` — 바이너리·데이터·로그 (repo 밖)
  - `kafka/`(설치), `kafka-data/`, `pg/data/`, `minio-data/`, `connect-plugins/`, `conf/`(렌더링된 설정), `logs/`, `pids/`, `iceberg-src/`(sink 빌드용 소스)

## 사용법

```bash
./deploy/install-runtime.sh          # 최초 1회: 바이너리 다운로드 + 플러그인 설치 (멱등)

./deploy/dzadmin all start           # 전체 기동: infra(pg→minio→kafka→connect) → backend → web
./deploy/dzadmin status              # 전체 상태 요약
./deploy/dzadmin all stop            # 전체 정지 (역순, 데이터 보존)
./deploy/dzadmin --help

./deploy/dzadmin backend restart     # 코드 반영 재기동 (별칭: engine)
./deploy/dzadmin web restart         # vite dev 서버 (--host 고정, 별칭: ui)
./deploy/smoke-test.sh               # 심층 헬스체크 (Connect 플러그인 3종 포함)
```

`dzadmin`은 기존 start/stop 스크립트의 통합 진입점이다 — 개별 스크립트도 그대로 쓸 수 있다.
`~/deltazium-runtime/bin/dzadmin` 심링크가 있어 PATH에 잡히면 어디서든 `dzadmin`으로 호출 가능.

로그 위치 (문제 추적 순서: UI 이벤트 탭 → 커넥터 status trace → 아래 파일):

| 로그 | 파일 |
|---|---|
| backend (등록·DDL·복구 API) | `~/deltazium-runtime/logs/backend.log` |
| Kafka Connect (커넥터 상세 — Debezium·Iceberg sink 포함) | `~/deltazium-runtime/logs/connect.log` |
| Kafka 브로커 | `~/deltazium-runtime/logs/server.log` (controller.log 등 동일 정책) |
| 복구 실행별 | `~/deltazium-runtime/logs/recovery-<테이블>-<id>.log` |

로테이션 정책: 오늘 로그는 위 파일에 쓰고, **일 단위 롤오버 시 `logs/yyyy-MM-dd/` 날짜
디렉터리로 이동**한다 (예: `logs/2026-08-03/connect-1.log`). 파일당 100MB 상한, 14일 보존.
`*-console.log`는 log4j/logback 초기화 전 기동 오류 캡처용 (기동마다 초기화, 평시 비어 있음).
설정 원본: `deploy/kafka/log4j2.yaml`, `deploy/connect/connect-log4j2.yaml`,
backend는 `application.yml`의 `logging` 절.

주의사항 (실험으로 확인, docs/experiments/2026-07-24-iceberg-sink-schema.md):
- Iceberg 배포판에 JDBC 드라이버 미포함 → install-runtime.sh가 plugin lib에 PG 드라이버 추가
- Connect worker에 `consumer.auto.offset.reset=earliest` 설정 — 커넥터 생성 전 발행분 유실 방지
- iceberg-sink는 idle 토픽에서 마지막 커밋이 다음 레코드 유입까지 지연될 수 있음

설정 변경은 `deploy/kafka/server.properties`, `deploy/connect/connect-distributed.properties` 템플릿을 수정 — start-infra.sh가 `${DZ_*}` 변수를 치환해 `~/deltazium-runtime/conf/`에 렌더링한다. 포트·경로는 전부 `deploy/env.sh`.

## 미결

- Oracle SRC/TGT 미연결 — 원격 dev Oracle 접속 정보 확정 대기. ARCHIVELOG + supplemental logging + LogMiner 권한 필요.
- 서버 재부팅 시 자동 기동 없음 (수동 start-infra.sh).

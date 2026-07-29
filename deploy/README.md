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
./deploy/install-runtime.sh  # 최초 1회: 바이너리 다운로드 + 플러그인 설치 (멱등)
./deploy/start-infra.sh      # 멱등 — 떠 있는 컴포넌트는 건너뜀
./deploy/smoke-test.sh       # 전 컴포넌트 + Connect 플러그인 3종 확인
./deploy/start-backend.sh    # 제어면 기동 (bootJar 빌드 포함, 재실행 = 재기동)
./deploy/stop-backend.sh
./deploy/stop-infra.sh       # 정지 (데이터 보존)
```

로그 위치 (문제 추적 순서: UI 이벤트 탭 → 커넥터 status trace → 아래 파일):

| 로그 | 파일 |
|---|---|
| backend (등록·DDL·복구 API) | `~/deltazium-runtime/logs/backend.log` |
| Kafka Connect (커넥터 상세) | `~/deltazium-runtime/logs/connect-YYYY-MM-DD-HH.log` |
| Kafka 브로커 | `~/deltazium-runtime/logs/kafka.log` |
| 복구 실행별 | `~/deltazium-runtime/logs/recovery-<테이블>-<id>.log` |

주의사항 (실험으로 확인, docs/experiments/2026-07-24-iceberg-sink-schema.md):
- Iceberg 배포판에 JDBC 드라이버 미포함 → install-runtime.sh가 plugin lib에 PG 드라이버 추가
- Connect worker에 `consumer.auto.offset.reset=earliest` 설정 — 커넥터 생성 전 발행분 유실 방지
- iceberg-sink는 idle 토픽에서 마지막 커밋이 다음 레코드 유입까지 지연될 수 있음

설정 변경은 `deploy/kafka/server.properties`, `deploy/connect/connect-distributed.properties` 템플릿을 수정 — start-infra.sh가 `${DZ_*}` 변수를 치환해 `~/deltazium-runtime/conf/`에 렌더링한다. 포트·경로는 전부 `deploy/env.sh`.

## 미결

- Oracle SRC/TGT 미연결 — 원격 dev Oracle 접속 정보 확정 대기. ARCHIVELOG + supplemental logging + LogMiner 권한 필요.
- 서버 재부팅 시 자동 기동 없음 (수동 start-infra.sh).

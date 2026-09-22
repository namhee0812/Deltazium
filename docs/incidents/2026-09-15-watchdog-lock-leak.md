# 2026-09-15 — 리부트 후 watchdog 락 상속으로 일주일 무감지 다운

## 요약

9/15 08:48경 SQueryDev 서버 리부트로 전 컴포넌트가 내려갔다. watchdog은 2분 뒤
정상적으로 DOWN을 감지했으나, 첫 재기동이 pg만 띄우고 실패했고 그 pg가 watchdog의
flock fd를 상속해 락을 영구 점유했다. 이후 5분 주기 실행이 전부 락 획득 실패로
무음 종료(exit 0)되어 minio·kafka·connect·web이 **9/22 14:29까지 일주일간 다운**된
채 방치됐다. 9/22 사용자가 UI(5173) 접속 불가를 보고해 발견.

## 타임라인

- 09-15 08:48경 — 서버 리부트 (`uptime` 역산)
- 09-15 08:50 — watchdog: `WARN infra DOWN 감지: pg minio kafka connect` →
  `dzadmin infra start` 실행. pg_ctl은 "server started"를 출력했으나 `wait_port`
  30초 내에 5433 listen이 안 잡혀 실패 판정 → 스크립트 중단(minio 이후 미기동).
  같은 주기에 backend는 재기동 성공.
- 09-15 08:55 이후 — watchdog 로그 무기록. 매 5분 실행됐으나 `flock -n` 실패로
  첫 줄에서 exit 0 (락은 08:50에 뜬 postgres가 fd 200으로 점유 — lsof로 확인).
- 09-22 14:2x — 사용자 보고로 발견, `dzadmin all start`로 전체 복구.

## 원인 (2건 복합)

1. **flock fd 상속**: `exec 200>"$LOCK_FILE"` 후 재기동한 데몬(nohup pg_ctl →
   postgres)이 fd 200을 상속. flock은 open file description에 붙으므로 데몬이 사는
   한 락이 풀리지 않고, `flock -n 200 || exit 0`이라 이후 실행이 로그 한 줄 없이
   종료됐다. 감시 도구가 자기 자신을 잠근 구조.
2. **pg 기동 대기 30초 부족**: 리부트는 pg 입장에서 비정상 종료라 WAL crash
   recovery가 돌고 부팅 직후 IO 경합까지 겹친다. listen까지 30초를 초과해
   `wait_port` 기본값으로는 실패 판정 → start-infra가 중단돼 나머지 컴포넌트를
   기동하지 못했다.

부차 요인: 리부트 시 자동 기동 경로가 없어 watchdog이 부팅 시퀀스 역할까지
떠맡고 있었다.

## 조치 (2026-09-22, 검증 완료)

- `watchdog.sh`: 재기동 명령을 `200>&-`로 락 fd를 닫고 실행 — 데몬 상속 차단.
- `start-infra.sh`: pg `wait_port` 타임아웃 30 → 120초 (crash recovery 대비).
- 물려 있던 락 해제: `/tmp/dz-watchdog.lock` 삭제(새 inode로 재생성, pg 재시작 불요).
- `@reboot sleep 30 && dzadmin all start` crontab 등록 — 리부트 후 web 포함 전체
  자동 기동. watchdog은 본래 역할(런타임 장애 재기동)로 복귀.
- 검증: minio 수동 정지 → watchdog 1회 실행으로 복구 확인 → `fuser` 락 점유
  프로세스 없음 → 2회차 실행 정상 진입.

## 교훈

- flock 기반 cron 스크립트가 데몬을 띄울 때는 락 fd 차단이 필수다. 실패 시
  무음(exit 0)인 경로에는 최소한의 흔적(감지 후 락 실패 로그 등)을 남기는 것도
  검토 대상.
- watchdog 자신이 죽거나 잠긴 경우를 감시하는 층이 없다 — 경고 센터는 backend가
  살아 있어야 뜨고, 이번엔 backend가 살아 있어 UI만 열었다면 kafka 경고를 볼 수
  있었으나 아무도 열지 않았다. 외부 채널 알림(Prometheus/Grafana 도입 시 재검토,
  TODO 모니터링 절)이 근본 대책.

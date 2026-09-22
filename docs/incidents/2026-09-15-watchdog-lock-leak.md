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

1. **cron PATH에 `/usr/sbin`이 없어 기동 판정 전멸**: start-infra의
   `is_up()`이 `ss -ltn`(EL8에서 `/usr/sbin/ss`) 기반인데 cron 기본 PATH는
   `/usr/bin:/bin`이라 `ss`가 command not found(stderr는 `2>/dev/null`로 삼켜짐)
   → `is_up`이 항상 false. 갓 띄운 pg가 실제로는 listen 중인데도 `wait_port`가
   타임아웃까지 전부 실패 판정 → start-infra 중단 → minio·kafka·connect 미기동.
   watchdog의 DOWN "감지"는 dzadmin `port_status`(bash 내장 `/dev/tcp`)라 cron에서도
   정상 동작한 것 — 같은 스택에서 감지와 기동 판정이 다른 방식이어서 반쪽만 죽었다.
   ※ 처음에는 "리부트 후 WAL crash recovery로 30초 초과"로 추정했으나, 9/22 복구
   리허설에서 타임아웃을 120초로 늘려도 동일 실패가 재현되어 PATH 문제로 확정.
2. **flock fd 상속**: `exec 200>"$LOCK_FILE"` 후 재기동한 데몬(nohup pg_ctl →
   postgres)이 fd 200을 상속. flock은 open file description에 붙으므로 데몬이 사는
   한 락이 풀리지 않고, `flock -n 200 || exit 0`이라 이후 실행이 로그 한 줄 없이
   종료됐다. 감시 도구가 자기 자신을 잠근 구조. 1번이 첫 재기동을 반쪽으로 만들고,
   2번이 재시도를 영구 봉쇄해 일주일 방치가 완성됐다.

부차 요인: 리부트 시 자동 기동 경로가 없어 watchdog이 부팅 시퀀스 역할까지
떠맡고 있었다.

## 조치 (2026-09-22)

- `start-infra.sh`: `is_up()`을 bash 내장 `/dev/tcp` 방식으로 교체(dzadmin
  `port_status`와 동일) — 외부 명령 의존 제거로 cron 환경에서도 판정 동작.
  pg `wait_port` 타임아웃 30 → 120초는 crash recovery 여유분으로 유지.
- `watchdog.sh`: 재기동 명령을 `200>&-`로 락 fd를 닫고 실행 — 데몬 상속 차단.
- 물려 있던 락 해제: `/tmp/dz-watchdog.lock` 삭제(새 inode로 재생성, pg 재시작 불요).
- `@reboot sleep 30 && dzadmin all start` crontab 등록 — 리부트 후 web 포함 전체
  자동 기동. watchdog은 본래 역할(런타임 장애 재기동)로 복귀.
- 검증: ① minio 수동 정지 → watchdog 수동 1회 실행으로 복구, `fuser` 락 점유 없음,
  2회차 정상 진입. ② 전체 stop 리허설 — 16:50 cron 주기에서 fd 상속 수정은 정상
  확인(backend 재기동 후 락 점유 없음), pg 판정 실패 재현으로 PATH 원인 확정.
  ③ is_up 수정 후 16:55 cron 주기의 전체 자동 복구로 최종 확인.

## 교훈

- cron에서 도는 스크립트는 대화형 셸과 PATH가 다르다. 판정 로직은 bash 내장이나
  절대 경로로 쓰고, 셸에서 수동 실행해 통과한 검증은 cron 검증을 대신하지 못한다
  (9/22 첫 검증이 정확히 그 함정 — 수동 실행은 통과, cron은 실패).
- 같은 스택의 "감지"와 "기동 판정"은 같은 방식이어야 한다. 방식이 다르면 감지만
  되고 기동은 안 되는 반쪽 장애가 생긴다.
- flock 기반 cron 스크립트가 데몬을 띄울 때는 락 fd 차단이 필수다. 실패 시
  무음(exit 0)인 경로에는 최소한의 흔적(감지 후 락 실패 로그 등)을 남기는 것도
  검토 대상.
- watchdog 자신이 죽거나 잠긴 경우를 감시하는 층이 없다 — 경고 센터는 backend가
  살아 있어야 뜨고, 이번엔 backend가 살아 있어 UI만 열었다면 kafka 경고를 볼 수
  있었으나 아무도 열지 않았다. 외부 채널 알림(Prometheus/Grafana 도입 시 재검토,
  TODO 모니터링 절)이 근본 대책.

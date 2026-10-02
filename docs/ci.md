# CI — GitLab + 계정별 shell 러너

SQueryDev(192.168.4.123) 한 대 위에 GitLab CE와 러너를 올려 deltazium(nhchoi)·deltastream(dstream)
두 프로젝트의 CI를 돌린다. 목적은 CI/CD 학습이며 시나리오를 점진적으로 늘린다 (2026-10-01 구축).

## 1. 구성

```
SQueryDev (OL 8.10, 8 vCPU, 31 GB)
├── [gitlab 계정] rootless podman
│     └── gitlab-ce 19.4.1 컨테이너   http :8929 / ssh :2222   데이터 /srv/gitlab/{config,logs,data}
├── [nhchoi 계정] gitlab-runner run (nohup, shell executor, tag: deltazium)
│     └── job → ~/gitlab-runner-builds/  ── 같은 계정의 ~/deltazium-runtime 스택(Kafka·Connect·PG·backend)에 접근
└── [dstream 계정] gitlab-runner run (nohup, shell executor, tag: deltastream)
```

| 항목 | 값 |
|---|---|
| GitLab URL | `http://192.168.4.123:8929` (관리자 `root`) |
| clone (SSH) | `ssh://git@192.168.4.123:2222/<namespace>/<project>.git` |
| 러너 등록 | 인스턴스 러너 2개, 태그로 라우팅, "Run untagged jobs" 해제 |
| 러너 버전 | gitlab-runner 19.4.1 (rpm, 리포 `packages.gitlab.com/runner/gitlab-runner`) |

## 2. 설계 판단

| 판단 | 근거 |
|---|---|
| GitLab을 별도 VM이 아니라 개발 서버에 | 새 VM도 같은 PC 위라 가용성 격리 효과가 없고 자원도 늘지 않는다. 학습 단계에선 VM 구축 비용이 더 크다. `gitlab-backup`으로 이전 가능해 되돌릴 수 있는 결정이다. 사내 공용으로 열게 되면 재검토 |
| GitLab 전용 OS 계정(`gitlab`) + rootless | 공용 서비스가 개인 계정에 묶이지 않게. 컨테이너 uid가 별도 subuid 범위(427680~)에 매핑돼 볼륨 소유권이 다른 계정과 섞이지 않는다. 일반 계정이라 침해 시 피해 범위가 계정 홈과 `/srv/gitlab`에 그친다 |
| Quadlet/systemd 대신 스크립트 | `dzadmin`과 같은 운용 방식. 재부팅 자동 기동이 필요해지면 그때 Quadlet으로 옮긴다(`podman run` 옵션이 1:1 대응) |
| 러너를 계정별로 | 러너 서비스 하나는 고정된 OS 유저 하나로 job을 돌린다. 두 프로젝트의 빌드·테스트가 각 계정 홈의 런타임·툴체인에 붙어야 하므로 계정별 프로세스가 필요하다. 바이너리는 공용 1개 |
| shell executor | job이 로컬 런타임(Kafka·Connect·PG·backend)을 직접 쓴다. docker executor는 빌드 이미지·네트워크 설계가 선행돼야 해 나중으로 미룸. `~/.gradle`·`~/.npm`을 그대로 쓰므로 `cache:` 설정이 필요 없다 |
| 러너 기동은 nohup 스크립트 | 학습 단계의 단순성. 자동 재시작·재부팅 기동이 필요해지면 `systemctl --user` + linger로 전환 |
| GitLab 사용자는 `root` 하나 | 운용 편의. 두 OS 계정이 같은 SSH 키를 쓰고 있어 GitLab에선 모두 root로 보인다. MR 승인 흐름 학습이 필요해지면 사용자 분리 |
| GitHub(`origin`)와 GitLab(`gitlab`) remote 분리 | 어느 push가 파이프라인을 트리거하는지 명시적으로 보이게. 이중 push URL은 GitLab이 주 저장소가 된 뒤 결정 |

## 3. 운용

### GitLab (gitlab 계정)

```bash
sudo -iu gitlab /srv/gitlab/gitlab.sh start|stop|restart|status|logs|rm|pull
sudo -iu gitlab podman exec gitlab gitlab-ctl status     # 내부 서비스 상태 (기동 확인은 이걸로)
curl -sI http://192.168.4.123:8929/users/sign_in          # 200이면 웹 정상
```

- `start`는 컨테이너가 있으면 `podman start`, 없으면 `podman run`. 설정 변경은 `rm` 후 `start`(볼륨은 유지).
- 첫 기동은 내부 reconfigure로 4~5분.
- `sudo -u gitlab podman ...`처럼 로그인 셸 없이 실행하면 `XDG_RUNTIME_DIR`이 없어 실패한다. `-i` 필수.
- nhchoi는 `/etc/sudoers.d/nhchoi-gitlab`(`nhchoi ALL=(gitlab) NOPASSWD: ALL`)로 gitlab 계정 전환만 가능(root 아님).

### 러너 (각 계정)

```bash
~/bin/runner-start.sh      # pid ~/.gitlab-runner/runner.pid, log ~/.gitlab-runner/runner.log
~/bin/runner-stop.sh       # SIGTERM 후 30초 대기, 이후 kill -9
tail -f ~/.gitlab-runner/runner.log
```

- 설정: `~/.gitlab-runner/config.toml` (`concurrent = 1`).
- 신규 러너: Admin → CI/CD → Runners → Create instance runner(태그 지정) → 표시된 `gitlab-runner register --url ... --token glrt-...`를 해당 계정에서 실행, executor `shell`.

### 최초 구축 절차 (요약)

1. root: `useradd -m -s /bin/bash gitlab`, `loginctl enable-linger gitlab`, `/srv/gitlab/{config,logs,data}` 생성·chown, `gitlab.sh` 배치.
2. gitlab 계정: `gitlab.sh pull` → `gitlab.sh start` → `/etc/gitlab/initial_root_password`로 로그인 후 비밀번호 변경.
3. root: 러너 rpm 설치 (`curl -L https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.rpm.sh | bash`, `dnf install gitlab-runner`). rpm이 만드는 시스템 서비스 `gitlab-runner.service`는 사용하지 않는다.
4. 각 계정: 인스턴스 러너 생성·register → `runner-start.sh`. root로 `loginctl enable-linger nhchoi dstream`(세션이 모두 끊겨도 러너 유지).
5. 각 계정: SSH 공개키를 GitLab root 사용자에 등록, `git remote add gitlab ...`, push.

## 4. 파이프라인 (deltazium — `.gitlab-ci.yml`)

```
push(모든 브랜치)      build ───────────▶ test
                       backend-build       backend-test   (JUnit → Tests 탭)
                       ui-build            ui-lint
main push / schedule                              ───▶ integration
                                                        e2e-cdc  (resource_group: dev-stack)
```

| job | stage | 내용 | 산출물 |
|---|---|---|---|
| `backend-build` | build | `./gradlew :backend:bootJar :recovery-job:jar` | `backend/build/libs/`, `recovery-job/build/libs/` (1주) |
| `ui-build` | build | `npm ci && npm run build` | `ui/dist/` (1주) |
| `backend-test` | test | `./gradlew test` | JUnit XML → 파이프라인 Tests 탭 (`when: always`) |
| `ui-lint` | test | `npm ci && npm run lint` (oxlint) | — |
| `e2e-cdc` | integration | `./deploy/e2e-cdc.sh` — PG→PG 전 구간(등록→스냅샷→DML→체크섬 정합→changelog) | — |

- `default.tags: [deltazium]` — 모든 job이 nhchoi 러너로 간다.
- ui job은 `before_script`에서 `source ~/.nvm/nvm.sh` — nvm 설치 node는 비로그인 셸 PATH에 없다.
- `cache:`를 쓰지 않는다 — shell executor는 `~/.gradle`·`~/.npm`을 그대로 쓴다.
- `e2e-cdc`는 `rules`로 main push·schedule에만 돈다. `resource_group`으로 같은 dev 스택을 쓰는 job이 동시에 돌지 않게 직렬화한다.
- E2E 스크립트의 단계·판정·정리 정책은 `docs/operations.md` "E2E 테스트" 절.

### 설계 원칙

- **job은 시나리오 단위가 아니라 실행 조건 단위**로 나눈다(환경·소요 시간·트리거가 다를 때). 시나리오는 테스트 코드·스크립트 안에서 늘린다.
- 단위 테스트(H2, 외부 의존 없음)는 push마다, 실 스택 E2E는 main·스케줄만. 복구 리허설(architecture.md 6.4)은 추후 별도 job(nightly/수동).
- 정합 검증은 at-least-once 전제(architecture.md 8절)로 최종 상태 비교(체크섬)이고, changelog는 "이벤트 수 이상"으로만 판정한다.

## 5. 한계·미해결

- **cgroup v1**: OL8 기본이 cgroup v1이라 rootless 컨테이너에 `--memory`/`--cpus` 상한이 무시되고 `podman stats`도 안 된다
  (기동 시 `Resource limits are not supported and ignored on cgroups V1 rootless systems`). GitLab 실사용은 약 3 GB.
  해결: `grubby --update-kernel=ALL --args="systemd.unified_cgroup_hierarchy=1"` + 재부팅(dstream의 SingleStore 컨테이너도 영향 — 시점 조율 필요).
- **재부팅 자동 기동 없음**: GitLab·러너 모두 재부팅 후 수동 기동.
- **CI가 테스트하는 backend**: integration job은 push된 코드가 아니라 `~/deltazium`에서 떠 있는 backend를 호출한다. 배포 stage(이번 커밋 jar로 재기동) 전까지 "이 커밋이 E2E를 통과했다"는 의미가 성립하지 않는다.
- 두 OS 계정이 같은 SSH 개인키를 공유한다. GitLab 사용자 분리 시 계정별 키로 교체.

## 6. 밟은 함정

| 증상 | 원인·조치 |
|---|---|
| `script.rpm.sh` 다운로드가 `404` → `bash: 404:: command not found` | 리포 경로는 `gitlab/gitlab-runner`가 아니라 `runner/gitlab-runner` |
| `podman ps`에 `(healthy)`가 끝내 안 뜸 | 이 이미지는 podman에서 healthcheck가 잡히지 않아 health 필드가 비어 있다. `gitlab-ctl status`·sign_in 200으로 판정 |
| SSH 키 등록 시 500 에러 | 키 문자열 오타(`ssh-sd25519`). Request ID로 `/var/log/gitlab/gitlab-rails/production_json.log`를 grep하면 `SSHData::AlgorithmError`가 보인다. 키는 반드시 `cat ~/.ssh/*.pub` 출력에서 직접 복사 |
| dstream 키 등록 시 `Fingerprint sha256 has already been taken` | dstream의 키 쌍이 nhchoi 것과 동일(복사본). 추가 등록 없이 그대로 인증된다 |
| 러너 등록 후 UI에 Offline | `register`는 등록만 한다. `gitlab-runner run` 프로세스가 떠 있어야 Online |
| `sudo bash -c '... > /etc/sudoers.d/...'`가 Permission denied | 붙여넣기 중 줄바꿈으로 명령이 깨진 것. 리다이렉트를 root로 하려면 `echo ... \| sudo tee 파일` |

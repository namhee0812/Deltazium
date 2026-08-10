#!/usr/bin/env bash
#
# Deltazium 절대 규칙 검사 — LLM 없이 grep으로 확인 가능한 위반만 잡는다.
# 판단이 필요한 리뷰는 이 스크립트의 범위가 아니다 (CLAUDE.md 리뷰 게이트 참고).
#
# 사용:
#   ./deploy/rule-check.sh            작업 트리 검사 + main 대비 변경 규모
#   ./deploy/rule-check.sh --staged   작업 트리 검사 + staged 기준 변경 규모 (pre-commit hook용)
#
# 종료 코드: 0 = 통과(경고는 통과), 1 = 절대 규칙 위반 → 커밋 차단

set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

VIOLATIONS=0
WARNINGS=0

red()  { printf '\033[31m%s\033[0m\n' "$*"; }
ylw()  { printf '\033[33m%s\033[0m\n' "$*"; }
grn()  { printf '\033[32m%s\033[0m\n' "$*"; }

# 위반: 커밋 차단
violation() {
    red "✗ 절대 규칙 위반: $1"
    printf '%s\n' "$2" | sed 's/^/    /'
    echo "    → 근거: $3"
    echo
    VIOLATIONS=$((VIOLATIONS + 1))
}

# 경고: 차단하지 않되 리뷰 게이트 판단에 쓴다
warning() {
    ylw "! $1"
    printf '%s\n' "$2" | sed 's/^/    /'
    echo
    WARNINGS=$((WARNINGS + 1))
}

echo "=== 절대 규칙 검사 ==="
echo

# ── 규칙 1: recovery-job은 타깃 DB에 직접 apply하지 않는다 (architecture.md 6절) ──
# Iceberg JDBC 카탈로그 접속(org.apache.iceberg.jdbc, jdbc.user 등)은 정상이므로 제외하고,
# 실제 apply 신호(커넥션 획득·SQL 실행·DML 리터럴)만 본다.
hits=$(grep -rnE \
    'import +java\.sql\.(Connection|Driver|Statement|PreparedStatement)|DriverManager|javax\.sql\.DataSource|\.executeUpdate\(|\.executeBatch\(|"(INSERT|MERGE|UPDATE|DELETE) +' \
    recovery-job/src/main --include='*.java' 2>/dev/null)
if [[ -n "$hits" ]]; then
    violation "recovery-job에 타깃 apply 코드" "$hits" \
        "복구는 재발행 방식만 — apply 시맨틱은 JDBC sink 단일 경로 (architecture.md 6절)"
fi

# ── 규칙 2: 데이터 경로는 기성 커넥터만. 커스텀 Kafka Connect 커넥터 금지 ──
hits=$(grep -rnE 'extends +(SourceConnector|SinkConnector|SourceTask|SinkTask)\b' \
    backend/src recovery-job/src --include='*.java' 2>/dev/null)
if [[ -n "$hits" ]]; then
    violation "커스텀 Kafka Connect 커넥터 작성" "$hits" \
        "자체 코드는 backend·recovery-job·ui 세 곳뿐 (CLAUDE.md 절대 규칙)"
fi

# ── 규칙 3: Spark/Trino 도입 금지 (architecture.md 5.3절) ──
# 주석(//, #)은 제외하고 실제 의존성 선언 줄만 본다.
hits=$(grep -rniE '^[[:space:]]*(implementation|api|runtimeOnly|compileOnly|testImplementation|testRuntimeOnly)[^/#]*(spark|trino)' \
    --include='build.gradle' --include='build.gradle.kts' . 2>/dev/null)
if [[ -n "$hits" ]]; then
    violation "Spark/Trino 의존성" "$hits" \
        "Iceberg 읽기는 iceberg-data(Java API) 단일 프로세스 (architecture.md 5.3절)"
fi

# ── 변경 범위 판정 ──
# staged 모드는 pre-commit에서, 아니면 main 대비로 본다.
if [[ "${1:-}" == "--staged" ]]; then
    DIFF_ARGS=(--cached)
    SCOPE="staged"
else
    if git rev-parse --verify -q main >/dev/null && [[ "$(git rev-parse HEAD)" != "$(git rev-parse main)" ]]; then
        DIFF_ARGS=(main...HEAD)
        SCOPE="main 대비"
    else
        DIFF_ARGS=(HEAD)
        SCOPE="작업 트리"
    fi
fi

changed=$(git diff --name-only "${DIFF_ARGS[@]}" 2>/dev/null)

# ── 규칙 4(경고): Iceberg changelog 스키마 고정 파일 변경 ──
# architecture.md 5절에 스키마·파티셔닝이 고정돼 있다. 변경 자체를 막지는 않되 반드시 눈에 띄게 한다.
SCHEMA_FILES='backend/src/main/java/io/deltazium/backend/iceberg/ChangelogTableService.java|connectors/iceberg-sink.json.tmpl'
schema_touched=$(printf '%s\n' "$changed" | grep -E "$SCHEMA_FILES" || true)
if [[ -n "$schema_touched" ]]; then
    warning "Iceberg changelog 스키마 고정 파일이 변경됨 (architecture.md 5절)" "$schema_touched"
fi

# ── 데이터 정합 경로 변경 여부 (리뷰 게이트 판단용) ──
INTEGRITY_PATHS='^recovery-job/|^connectors/|/iceberg/|/recovery/|/registration/'
integrity_touched=$(printf '%s\n' "$changed" | grep -E "$INTEGRITY_PATHS" || true)

# ── 변경 규모 ──
files=$(printf '%s\n' "$changed" | grep -c . || true)
lines=$(git diff --numstat "${DIFF_ARGS[@]}" 2>/dev/null \
    | awk '{ a += ($1 == "-" ? 0 : $1); d += ($2 == "-" ? 0 : $2) } END { print a + d + 0 }')

echo "=== 변경 규모 ($SCOPE) ==="
echo "  파일 ${files}개 / ${lines}줄"
if [[ -n "$integrity_touched" ]]; then
    echo "  데이터 정합 경로 포함:"
    printf '%s\n' "$integrity_touched" | sed 's/^/    /'
fi

# 리뷰 게이트 권고 — 판단은 사람(메인 세션)이 한다. 여기서는 근거 숫자만 제시.
echo
echo "=== 리뷰 게이트 권고 ==="
if [[ -n "$integrity_touched" || -n "$schema_touched" ]]; then
    echo "  데이터 정합 경로 변경 → 병합 전 diff 직접 검토 필수"
    if (( files > 5 || lines > 200 )); then
        echo "  규모가 큼 → 리뷰어 위임 검토 (메인 컨텍스트에 전체 diff를 얹지 않기 위함)"
    fi
elif (( files > 5 || lines > 200 )); then
    echo "  일반 기능이지만 규모가 큼 → 리뷰어 위임 검토"
else
    echo "  일반/기계적 변경 규모 → 추가 게이트 없음"
fi
echo

if (( VIOLATIONS > 0 )); then
    red "=== 위반 ${VIOLATIONS}건 — 커밋 차단 ==="
    echo "규칙 자체가 잘못됐다고 판단되면 스크립트를 고치지 말고 사용자에게 '결정 필요'로 보고할 것."
    exit 1
fi

if (( WARNINGS > 0 )); then
    grn "절대 규칙 위반 없음 (경고 ${WARNINGS}건)"
else
    grn "절대 규칙 위반 없음"
fi
exit 0

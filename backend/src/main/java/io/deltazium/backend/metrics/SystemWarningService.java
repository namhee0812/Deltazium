package io.deltazium.backend.metrics;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.deltazium.backend.connect.ConnectClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 파일명 : SystemWarningService.java
 * 작성일자 : 26. 08. 24.
 * 작성자 : 최남희
 * 설명 : 전역 "경고 센터" 집계 — 디스크 사용률·Kafka 연결·커넥터 상태 3종을 점검해
 * UI 헤더 경고 칩에 노출한다. 26-08-20 디스크 풀로 Kafka가 죽고 나흘간 미검출된
 * 장애의 재발 방지가 목적이므로, 이 서비스 자체는 Kafka·Connect가 죽어 있어도
 * (그 사실을 알리기 위해) 예외 없이 응답해야 한다 — 점검 항목별로 독립
 * try/catch로 감싸 한 항목의 실패가 전체 응답을 막지 않게 한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 24.       | 최남희  | 최초 생성
 * 26. 08. 24.       | 최남희  | 커넥터 RUNNING인데 task 0개인 상태를 WARN으로 판정 (소비 정지 맹점)
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | severity 3단(CRITICAL/WARN/INFO) 도입 — 사람이 정지한 PAUSED
 * |                          | 커넥터를 WARN에서 INFO로 분리하고 확인(ack) API를 추가했다.
 * |                          | INFO 항목의 sinceMs는 system_warning_acks(DB)로 안정화해 backend
 * |                          | 재기동 후에도 같은 정지 건에 대한 ack가 유지되게 한다(내부 판단
 * |                          | 근거: docs/internals.md).
 * --------------------------------------------------
 */
@Service
public class SystemWarningService {

    private static final Logger log = LoggerFactory.getLogger(SystemWarningService.class);

    public record SystemWarning(String id, String severity, String title, String detail, Long sinceMs) {
    }

    public record SystemWarningsResponse(List<SystemWarning> warnings) {
    }

    private final KafkaMetricsService metrics;
    private final ConnectClient connect;
    private final SystemWarningAckRepository acks;
    private final String runtimeDir;
    private final int diskWarnPct;
    /** 경고 id → 최초 감지 시각(epoch ms). 해소되면 다음 조회에서 제거한다. backend 재기동 시 리셋됨.
     *  INFO 항목은 이 값을 쓰지 않는다 — sinceMs 안정화는 system_warning_acks(DB)가 대신한다. */
    private final Map<String, Long> firstSeen = new ConcurrentHashMap<>();
    /** 직전 조회에서 관측된 INFO 경고 id 집합 — 다음 조회에서 사라진 id는 해소된 것으로 보고
     *  system_warning_acks 관측 행을 지운다(재정지 시 sinceMs를 새로 시작시키기 위함). */
    private volatile Set<String> knownInfoIds = Set.of();

    public SystemWarningService(KafkaMetricsService metrics,
                                ConnectClient connect,
                                SystemWarningAckRepository acks,
                                @Value("${deltazium.runtime-dir}") String runtimeDir,
                                @Value("${deltazium.disk-warn-pct:85}") int diskWarnPct) {
        this.metrics = metrics;
        this.connect = connect;
        this.acks = acks;
        this.runtimeDir = runtimeDir;
        this.diskWarnPct = diskWarnPct;
    }

    /** ack되지 않은 경고만 — INFO 중 ack.sinceMs == 항목.sinceMs인 것은 제외한다. */
    public SystemWarningsResponse warnings() {
        List<SystemWarning> active = computeActive();
        List<SystemWarning> visible = active.stream()
                .filter(w -> !isAcked(w))
                .toList();
        return new SystemWarningsResponse(visible);
    }

    /**
     * 확인(ack) — INFO 등급만 허용한다. 대상 id가 현재 활성 목록에 없으면(이미 해소됐거나 존재한
     * 적 없으면) 404에 해당하는 예외, INFO가 아니면 400에 해당하는 예외를 던진다.
     */
    public void ack(String id) {
        SystemWarning target = computeActive().stream()
                .filter(w -> w.id().equals(id))
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("해당 알림을 찾을 수 없습니다: " + id));
        if (!"INFO".equals(target.severity())) {
            throw new IllegalArgumentException("INFO 등급 알림만 확인 처리할 수 있습니다: " + id);
        }
        acks.ack(id);
    }

    private boolean isAcked(SystemWarning w) {
        if (!"INFO".equals(w.severity())) {
            return false;
        }
        return acks.findAll().stream()
                .anyMatch(o -> o.id().equals(w.id()) && o.acked() && o.sinceMs() == w.sinceMs());
    }

    /** ack 필터 전 원시 활성 경고 목록 — INFO 항목의 sinceMs는 이미 DB로 안정화된 값이다. */
    private List<SystemWarning> computeActive() {
        long now = System.currentTimeMillis();
        List<SystemWarning> active = new ArrayList<>();

        try {
            checkDisk(active, now);
        } catch (Exception e) {
            log.warn("디스크 사용률 점검 실패", e);
        }
        try {
            checkKafka(active, now);
        } catch (Exception e) {
            log.warn("Kafka 연결 점검 실패", e);
        }
        try {
            checkConnectors(active, now);
        } catch (Exception e) {
            log.warn("커넥터 상태 점검 실패", e);
        }

        // 이번 조회에서 잡히지 않은(해소된) 경고는 firstSeen에서 제거 — 재발 시 sinceMs가 새로 시작되게.
        Set<String> activeIds = active.stream().map(SystemWarning::id).collect(Collectors.toSet());
        firstSeen.keySet().retainAll(activeIds);

        return stabilizeInfoSinceMs(active, now);
    }

    /**
     * INFO 항목의 sinceMs를 system_warning_acks(DB)로 안정화한다 — 최초 관측 시 관측 행을 만들고,
     * 이후엔 그 행의 since_ms를 그대로 쓴다(backend 재기동으로 in-memory firstSeen이 리셋돼도
     * 같은 정지 건에 대한 ack 유효성이 깨지지 않게 하기 위함). 더 이상 활성이 아닌(재개된) INFO
     * id의 관측 행은 지운다 — 같은 커넥터가 재개 후 다시 정지되면 sinceMs가 새로 시작된다.
     */
    private List<SystemWarning> stabilizeInfoSinceMs(List<SystemWarning> active, long now) {
        boolean anyInfo = active.stream().anyMatch(w -> "INFO".equals(w.severity()));
        if (!anyInfo && knownInfoIds.isEmpty()) {
            return active;
        }

        Map<String, WarningObservation> observed = acks.findAll().stream()
                .collect(Collectors.toMap(WarningObservation::id, o -> o));

        List<SystemWarning> result = new ArrayList<>(active.size());
        Set<String> currentInfoIds = new HashSet<>();
        for (SystemWarning w : active) {
            if (!"INFO".equals(w.severity())) {
                result.add(w);
                continue;
            }
            currentInfoIds.add(w.id());
            WarningObservation obs = observed.get(w.id());
            long sinceMs = obs != null ? obs.sinceMs() : now;
            if (obs == null) {
                acks.insertObserved(w.id(), now);
            }
            result.add(new SystemWarning(w.id(), w.severity(), w.title(), w.detail(), sinceMs));
        }

        Set<String> resolved = new HashSet<>(knownInfoIds);
        resolved.removeAll(currentInfoIds);
        if (!resolved.isEmpty()) {
            acks.deleteIds(new ArrayList<>(resolved));
        }
        knownInfoIds = currentInfoIds;

        return result;
    }

    /** 원래 스펙의 디스크 경고 API(GET /api/metrics/system)를 대체 — deltazium.runtime-dir 파일시스템 사용률. */
    private void checkDisk(List<SystemWarning> out, long now) {
        File dir = new File(runtimeDir);
        long total = dir.getTotalSpace();
        if (total <= 0) {
            return; // 경로 접근 불가 — 판정 불가, 조용히 스킵 (총량 0 반환은 권한 문제 등)
        }
        long usable = dir.getUsableSpace();
        int usedPct = (int) Math.round((total - usable) * 100.0 / total);
        if (usedPct >= diskWarnPct) {
            addWarning(out, now, "disk-usage", "WARN",
                    "디스크 사용률 " + usedPct + "%",
                    "사용률 " + usedPct + "% (임계 " + diskWarnPct + "%) — 경로: " + runtimeDir);
        }
    }

    /** KafkaMetricsService의 AdminClient(짧은 타임아웃)를 재사용해 describeCluster 왕복만 확인. */
    private void checkKafka(List<SystemWarning> out, long now) {
        if (!metrics.reachable()) {
            addWarning(out, now, "kafka-unreachable", "CRITICAL",
                    "Kafka 연결 불가", "AdminClient describeCluster 실패(타임아웃 포함) — Kafka 브로커 상태 확인 필요");
        }
    }

    /** Connect REST로 전체 커넥터 상태 조회. REST 자체가 죽어 있으면 개별 커넥터 대신 단일 경고. */
    private void checkConnectors(List<SystemWarning> out, long now) {
        JsonNode all;
        try {
            all = connect.listConnectors();
        } catch (Exception e) {
            addWarning(out, now, "connect-unreachable", "CRITICAL",
                    "Kafka Connect 연결 불가", "Connect REST 조회 실패: " + e.getMessage());
            return;
        }
        Iterator<String> names = all.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            JsonNode status = all.get(name).path("status");
            String state = effectiveState(status);
            if ("FAILED".equals(state)) {
                addWarning(out, now, "connector-failed:" + name, "CRITICAL",
                        name + " 실패", "커넥터 또는 태스크가 FAILED 상태입니다");
            } else if ("PAUSED".equals(state)) {
                // 사람이 직접 정지했거나(테이블 정지) DDL 거부로 apply만 멈춘 상태 — 고장이 아니라
                // 확인 후 없앨 수 있는 알림(INFO)이다. changelog 축적은 계속된다.
                addWarning(out, now, "connector-paused:" + name, "INFO",
                        name + " 정지됨", "사용자 정지 또는 DDL 거부로 apply가 정지된 상태 — changelog 축적은 계속됩니다");
            } else if (!"RUNNING".equals(state)) {
                addWarning(out, now, "connector-degraded:" + name, "WARN",
                        name + " 비정상 상태", "현재 상태: " + state);
            } else if (status.path("tasks").isEmpty()) {
                // 커넥터 RUNNING이라도 task가 하나도 없으면 소비가 멈춘 상태다
                // (리밸런스 직후 일시 상태일 수 있으나, 지속되면 개입이 필요하므로 WARN)
                addWarning(out, now, "connector-no-tasks:" + name, "WARN",
                        name + " task 없음", "커넥터는 RUNNING이지만 task가 0개 — 소비 정지 상태");
            }
        }
    }

    /** 커넥터 자체 또는 태스크 중 하나라도 FAILED면 FAILED로 본다 (ConnectorHealthWatcher와 동일 판정). */
    private static String effectiveState(JsonNode status) {
        String state = status.path("connector").path("state").asText("UNKNOWN");
        for (JsonNode task : status.path("tasks")) {
            if ("FAILED".equals(task.path("state").asText())) {
                return "FAILED";
            }
        }
        return state;
    }

    private void addWarning(List<SystemWarning> out, long now,
                            String id, String severity, String title, String detail) {
        long since = firstSeen.computeIfAbsent(id, k -> now);
        out.add(new SystemWarning(id, severity, title, detail, since));
    }
}

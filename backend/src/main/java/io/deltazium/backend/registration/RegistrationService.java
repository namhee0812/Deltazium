package io.deltazium.backend.registration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.dictionary.OracleDictionaryService;
import io.deltazium.backend.dictionary.SourceTableInfo;
import io.deltazium.backend.dictionary.TableColumn;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.iceberg.ChangelogTableService;
import io.deltazium.backend.iceberg.IcebergProperties;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * CDC 테이블 등록 (architecture.md 8절).
 * 흐름: 딕셔너리 조회 → 사전 점검(PK 필수, supp.log ALL, 권한) → 컬럼 매핑 검증 →
 * 메타데이터 저장 → 커넥터 배포.
 *
 * 커넥터 구성: source·iceberg-sink는 전역 1개, jdbc-sink는 **테이블별 1개**
 * (dz-jdbc-sink-<suffix>) — 타깃 테이블명 매핑과 테이블 단위 정지(7절)를 위해.
 */
@Service
public class RegistrationService {

    /** 등록 요청의 테이블 한 건. target·columns가 비면 소스와 동일/전 컬럼으로 저장한다. */
    public record TableSpec(String source, String targetSchema, String targetTable,
                            List<ColumnMapping> columns) {
    }

    private final RegisteredTableRepository repository;
    private final RegisteredColumnRepository columnRepository;
    private final DbConnectionService connections;
    private final OracleDictionaryService dictionary;
    private final ConnectorDeployService deploy;
    private final ChangelogTableService changelog;
    private final IcebergProperties iceberg;
    private final TableEventService events;
    private final String kafkaBootstrap;
    private final String topicPrefix;

    public RegistrationService(RegisteredTableRepository repository,
                               RegisteredColumnRepository columnRepository,
                               DbConnectionService connections,
                               OracleDictionaryService dictionary,
                               ConnectorDeployService deploy,
                               ChangelogTableService changelog,
                               IcebergProperties iceberg,
                               TableEventService events,
                               @Value("${deltazium.kafka.bootstrap}") String kafkaBootstrap,
                               @Value("${deltazium.topic-prefix}") String topicPrefix) {
        this.repository = repository;
        this.columnRepository = columnRepository;
        this.connections = connections;
        this.dictionary = dictionary;
        this.deploy = deploy;
        this.changelog = changelog;
        this.iceberg = iceberg;
        this.events = events;
        this.kafkaBootstrap = kafkaBootstrap;
        this.topicPrefix = topicPrefix;
    }

    public List<RegisteredTable> list() {
        return repository.findAll();
    }

    public List<ColumnMapping> mappings(long registeredTableId) {
        return columnRepository.findByTable(registeredTableId);
    }

    /** 소스 딕셔너리에서 패턴에 걸리는 테이블 + 점검 상태 조회 (등록 후보). */
    public List<SourceTableInfo> discover(long sourceConnectionId, String pattern) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        return dictionary.listTables(source, pattern);
    }

    /** 컬럼 목록 (소스/타깃 어느 연결이든) — 매핑 화면용. */
    public List<TableColumn> columns(long connectionId, String qualifiedTable) {
        int dot = qualifiedTable.indexOf('.');
        if (dot <= 0) {
            throw new IllegalArgumentException("SCHEMA.TABLE 형식이어야 한다: " + qualifiedTable);
        }
        return dictionary.listColumns(connections.get(connectionId),
                qualifiedTable.substring(0, dot), qualifiedTable.substring(dot + 1));
    }

    /** DB 레벨 사전 점검 (ARCHIVELOG, DB supplemental logging). */
    public Map<String, String> databaseChecks(long sourceConnectionId) {
        return dictionary.databaseChecks(requireRole(sourceConnectionId, "SOURCE"));
    }

    /** LogMiner 권한 점검 — 누락 시 UI가 DBA용 GRANT 스크립트 안내 (자체 적용 불가). */
    public Map<String, Boolean> privilegeChecks(long sourceConnectionId) {
        return dictionary.privilegeChecks(requireRole(sourceConnectionId, "SOURCE"));
    }

    /** 사용자가 UI에서 승인한 경우에만 호출 — 테이블별 supp.log(ALL) 적용 시도. */
    public Map<String, String> applySupplementalLogging(long sourceConnectionId, List<String> tables) {
        return dictionary.applySupplementalLogging(requireRole(sourceConnectionId, "SOURCE"), tables);
    }

    @Transactional
    public List<RegisteredTable> register(long sourceConnectionId, long targetConnectionId,
                                          List<TableSpec> specs) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        DbConnection target = requireRole(targetConnectionId, "TARGET");
        if (specs == null || specs.isEmpty()) {
            throw new IllegalArgumentException("등록할 테이블이 없다");
        }

        // 검증 후 저장 — 하나라도 실패하면 전체 롤백
        for (TableSpec spec : specs) {
            SourceTableInfo info = validateTable(source, spec.source());
            List<TableColumn> sourceColumns = dictionary.listColumns(source, info.schema(), info.table());
            List<ColumnMapping> mappings = normalizeMappings(spec, sourceColumns);

            long tableId = repository.insert(info.schema(), info.table(),
                    sourceConnectionId, targetConnectionId,
                    upperOrNull(spec.targetSchema()), upperOrNull(spec.targetTable()));
            columnRepository.insertAll(tableId, mappings);
        }

        deployConnectors(source, target);
        for (TableSpec spec : specs) {
            int dot = spec.source().indexOf('.');
            events.info(spec.source().substring(0, dot).toUpperCase(Locale.ROOT),
                    spec.source().substring(dot + 1).toUpperCase(Locale.ROOT),
                    "REGISTERED", "CDC 등록·커넥터 배포 (source: " + source.name()
                            + " → target: " + target.name() + ")");
        }
        return repository.findAll();
    }

    private SourceTableInfo validateTable(DbConnection source, String qualified) {
        if (qualified == null || qualified.contains("*") || qualified.contains("%")) {
            throw new IllegalArgumentException("와일드카드는 등록 시점에 허용되지 않는다: " + qualified);
        }
        List<SourceTableInfo> found = dictionary.listTables(source, qualified);
        if (found.isEmpty()) {
            throw new IllegalArgumentException("소스에 존재하지 않는 테이블: " + qualified);
        }
        SourceTableInfo info = found.get(0);
        if (!info.hasPk()) {
            throw new IllegalArgumentException("PK 없는 테이블은 등록 불가 (멱등 upsert 전제): " + qualified);
        }
        if (!info.suppLogAll()) {
            throw new IllegalArgumentException("supplemental logging (ALL) COLUMNS 미설정: " + qualified
                    + " — 사전 점검 단계에서 적용 후 다시 시도");
        }
        if (repository.exists(info.schema(), info.table())) {
            throw new IllegalArgumentException("이미 등록된 테이블: " + qualified);
        }
        if (repository.existsTableNameInOtherSchema(info.schema(), info.table())) {
            throw new IllegalArgumentException(
                    "다른 스키마에 같은 이름의 테이블이 이미 등록됨: " + info.table()
                    + " — iceberg 라우팅(route-field=source.table) 충돌로 동시 등록 불가 (5.1절)");
        }
        return info;
    }

    /**
     * 매핑 검증·기본값 생성.
     * - 미지정 시: 소스 전 컬럼 동일명 매핑(enabled).
     * - 구문(`${COL}`)·소스 컬럼 존재·PK 규칙(소스 PK 전부 enabled + 동일명) 검증.
     */
    private List<ColumnMapping> normalizeMappings(TableSpec spec, List<TableColumn> sourceColumns) {
        Set<String> sourceNames = sourceColumns.stream()
                .map(c -> c.name().toUpperCase(Locale.ROOT)).collect(Collectors.toSet());
        Set<String> pkNames = sourceColumns.stream().filter(TableColumn::pk)
                .map(c -> c.name().toUpperCase(Locale.ROOT)).collect(Collectors.toSet());

        List<ColumnMapping> mappings = spec.columns();
        if (mappings == null || mappings.isEmpty()) {
            return sourceColumns.stream()
                    .map(c -> new ColumnMapping(c.name(), "${" + c.name() + "}", true))
                    .toList();
        }

        Set<String> enabledIdentityCols = new java.util.HashSet<>();
        List<ColumnMapping> normalized = new ArrayList<>();
        for (ColumnMapping m : mappings) {
            if (m.targetColumn() == null || m.targetColumn().isBlank()) {
                throw new IllegalArgumentException("타깃 컬럼명이 비어 있다");
            }
            if (!m.enabled()) {
                normalized.add(m);
                continue;
            }
            String srcCol = m.sourceColumn().orElseThrow(() -> new IllegalArgumentException(
                    "변환식 구문 오류 (허용 형식: ${소스컬럼}): " + m.targetColumn() + " ← " + m.sourceExpr()));
            if (!sourceNames.contains(srcCol)) {
                throw new IllegalArgumentException(
                        "소스에 없는 컬럼을 참조한다: " + m.sourceExpr() + " (" + spec.source() + ")");
            }
            if (m.isIdentity()) {
                enabledIdentityCols.add(srcCol);
            }
            normalized.add(m);
        }
        if (!enabledIdentityCols.containsAll(pkNames)) {
            throw new IllegalArgumentException(
                    "소스 PK 컬럼은 전부 동일명으로 매핑·활성화돼야 한다 (upsert key 전제): PK=" + pkNames);
        }
        return normalized;
    }

    private static String upperOrNull(String s) {
        return s == null || s.isBlank() ? null : s.trim().toUpperCase(Locale.ROOT);
    }

    /** 등록 전체 목록 기준으로 source·iceberg-sink(전역)·jdbc-sink(테이블별) 갱신 배포. */
    private void deployConnectors(DbConnection source, DbConnection target) {
        List<RegisteredTable> all = repository.findAll();
        String includeList = all.stream().map(RegisteredTable::qualified)
                .collect(Collectors.joining(","));
        String topics = all.stream().map(t -> topicPrefix + "." + t.qualified())
                .collect(Collectors.joining(","));

        for (RegisteredTable t : all) {
            changelog.ensureChangelogTable(t.schemaName(), t.tableName());
        }

        Map<String, String> sourceVars = new HashMap<>();
        sourceVars.put("connector_name", "dz-source");
        sourceVars.put("oracle_host", source.host());
        sourceVars.put("oracle_port", String.valueOf(source.port()));
        sourceVars.put("oracle_user", source.username());
        sourceVars.put("oracle_password", source.password());
        sourceVars.put("oracle_dbname", source.databaseName());
        sourceVars.put("topic_prefix", topicPrefix);
        sourceVars.put("table_include_list", includeList);
        sourceVars.put("kafka_bootstrap", kafkaBootstrap);
        deploy.deploy("source", sourceVars);

        // 구버전 단일 jdbc-sink가 남아있으면 제거 (테이블별 커넥터로 전환됨)
        try {
            deployLegacyCleanup();
        } catch (Exception ignored) {
            // 없으면 그만 — 배포 흐름을 막지 않는다
        }

        // jdbc-sink: 테이블별 커넥터 — 타깃 이름 매핑 + 컬럼 선택(field.include.list) 반영
        for (RegisteredTable t : all) {
            List<ColumnMapping> mappings = columnRepository.findByTable(t.id());
            Map<String, String> vars = new HashMap<>();
            vars.put("connector_name", "dz-jdbc-sink-" + t.suffix());
            vars.put("topics", topicPrefix + "." + t.qualified());
            vars.put("target_jdbc_url", target.jdbcUrl());
            vars.put("target_user", target.username());
            vars.put("target_password", target.password());
            vars.put("collection_name", t.targetQualified());
            deploy.deploy("jdbc-sink", vars, fieldIncludeConfig(mappings));
        }

        Map<String, String> icebergVars = new HashMap<>();
        icebergVars.put("connector_name", "dz-iceberg-sink");
        icebergVars.put("topics", topics);
        icebergVars.put("catalog_jdbc_url", iceberg.catalogUri());
        icebergVars.put("catalog_jdbc_user", iceberg.catalogUser());
        icebergVars.put("catalog_jdbc_password", iceberg.catalogPassword());
        icebergVars.put("warehouse", iceberg.warehouse());
        icebergVars.put("s3_endpoint", iceberg.s3Endpoint());
        icebergVars.put("s3_access_key", iceberg.s3AccessKey());
        icebergVars.put("s3_secret_key", iceberg.s3SecretKey());
        icebergVars.put("iceberg_tables", all.stream()
                .map(t -> changelog.changelogTableName(t.schemaName(), t.tableName()))
                .collect(Collectors.joining(",")));
        Map<String, String> routeRegex = new HashMap<>();
        for (RegisteredTable t : all) {
            routeRegex.put("iceberg.table."
                    + changelog.changelogTableName(t.schemaName(), t.tableName()) + ".route-regex",
                    "^" + t.tableName() + "$");
        }
        deploy.deploy("iceberg-sink", icebergVars, routeRegex);
    }

    /**
     * 컬럼 선택 실반영 — enabled + 동일명 매핑만 field.include.list에 넣는다.
     * 리네임(비동일명)은 스톡 sink가 지원하지 않아 저장만 하고 적재에서 제외한다
     * (전 컬럼 동일명·전체 활성이면 필터 불필요 — 키 자체를 생략).
     */
    public static Map<String, String> fieldIncludeConfig(List<ColumnMapping> mappings) {
        if (mappings.isEmpty()) {
            return Map.of();
        }
        List<String> included = mappings.stream()
                .filter(m -> m.enabled() && m.isIdentity())
                .map(m -> m.sourceColumn().orElseThrow())
                .toList();
        boolean allIdentityEnabled = included.size() == mappings.size();
        if (allIdentityEnabled) {
            return Map.of();
        }
        return Map.of("field.include.list", String.join(",", included));
    }

    private void deployLegacyCleanup() {
        deploy.deleteConnector("dz-jdbc-sink");
    }

    /** 일시 정지 — 해당 테이블 apply만 멈춘다. 캡처·changelog 축적은 계속(재개 시 캐치업). */
    public void pause(long registeredTableId) {
        RegisteredTable t = find(registeredTableId);
        deploy.pauseConnector("dz-jdbc-sink-" + t.suffix());
        events.info(t.schemaName(), t.tableName(), "PAUSED",
                "apply 정지 — 캡처·changelog 축적은 계속");
    }

    public void resume(long registeredTableId) {
        RegisteredTable t = find(registeredTableId);
        deploy.resumeConnector("dz-jdbc-sink-" + t.suffix());
        events.info(t.schemaName(), t.tableName(), "RESUMED", "apply 재개 — 밀린 분부터 캐치업");
    }

    /**
     * 등록 해제 — 커넥터에서 제거 + 메타데이터 삭제.
     * @param dropChangelog true면 changelog(Iceberg/S3) 데이터까지 삭제 —
     *                      복구 원본이 사라지므로 UI에서 명시 확인을 받은 값이어야 한다. 기본 보존.
     */
    @Transactional
    public List<RegisteredTable> unregister(long registeredTableId, boolean dropChangelog) {
        RegisteredTable table = find(registeredTableId);
        columnRepository.deleteByTable(registeredTableId);
        repository.delete(registeredTableId);

        // 테이블별 sink 제거 (recovery-sink는 있을 때만)
        quietDelete("dz-jdbc-sink-" + table.suffix());
        quietDelete("dz-recovery-sink-" + table.suffix());

        List<RegisteredTable> remaining = repository.findAll();
        if (remaining.isEmpty()) {
            // 삭제 전에 offset 정리 (offset 삭제는 커넥터가 STOPPED로 존재해야 가능) —
            // 같은 이름 재등록 시 스냅샷 SKIP 방지 (실측: NH_MIX_TABLE_01 재등록 시 SKIPPED)
            resetConnectorOffsets("dz-source");
            quietDelete("dz-source");
            quietDelete("dz-iceberg-sink");
        } else {
            // 남은 테이블 기준으로 source include list·iceberg 라우팅 재배포
            RegisteredTable any = remaining.get(0);
            deployConnectors(connections.get(any.sourceConnectionId()),
                    connections.get(any.targetConnectionId()));
        }

        if (dropChangelog) {
            changelog.dropChangelogTable(table.schemaName(), table.tableName(), true);
        }
        events.info(table.schemaName(), table.tableName(), "UNREGISTERED",
                "등록 해제 — changelog " + (dropChangelog ? "삭제됨" : "보존"));
        return remaining;
    }

    private void resetConnectorOffsets(String connector) {
        try {
            deploy.stopAndResetOffsets(connector);
        } catch (Exception ignored) {
            // 커넥터 미존재 등 — 재등록 시 스냅샷 SKIP 가능성만 남고 치명적이지 않음
        }
    }

    private void quietDelete(String connector) {
        try {
            deploy.deleteConnector(connector);
        } catch (Exception ignored) {
            // 미배포 커넥터 — 무시
        }
    }

    private RegisteredTable find(long registeredTableId) {
        return repository.findAll().stream()
                .filter(t -> t.id() == registeredTableId).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("등록 테이블 없음: id=" + registeredTableId));
    }

    private DbConnection requireRole(long connectionId, String role) {
        DbConnection c = connections.get(connectionId);
        if (!role.equals(c.role())) {
            throw new IllegalArgumentException(
                    "%s 역할 연결이 필요하다: %s(role=%s)".formatted(role, c.name(), c.role()));
        }
        return c;
    }
}

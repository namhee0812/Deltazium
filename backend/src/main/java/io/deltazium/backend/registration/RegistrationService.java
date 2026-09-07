package io.deltazium.backend.registration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.connect.ConnectorNames;
import io.deltazium.backend.dictionary.DictionaryRouter;
import io.deltazium.backend.dictionary.PrecheckItem;
import io.deltazium.backend.dictionary.SourceDictionary;
import io.deltazium.backend.dictionary.SourceTableInfo;
import io.deltazium.backend.dictionary.TableColumn;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.iceberg.ChangelogTableService;
import io.deltazium.backend.iceberg.IcebergProperties;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.DbType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 파일명 : RegistrationService.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : CDC 테이블 등록 (architecture.md 8절).
 * 흐름: 딕셔너리 조회 → 사전 점검(PK 필수, 캡처 설정, 권한) → 컬럼 매핑 검증 →
 * 메타데이터 저장 → 커넥터 배포.
 * 커넥터 구성: source·iceberg-sink는 **소스 커넥션별 1개**(dz-source-&lt;prefix&gt;·
 * dz-iceberg-&lt;prefix&gt;, 4절), jdbc-sink는 **테이블별 1개**
 * (dz-jdbc-sink-&lt;prefix&gt;-&lt;suffix&gt;) — 타깃 테이블명 매핑과 테이블 단위 정지(7절)를 위해.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | resnapshot 추가 — 캡처 재스냅샷(offset 리셋 + snapshot.mode
 * |                          | 오버라이드 재배포 + 재개), deployConnectors 오버라이드 오버로드
 * --------------------------------------------------
 * 26. 08. 05.       | 최남희  | resnapshot 오케스트레이션을 ResnapshotOrchestrator로 이관 —
 * |                          | 여기는 redeployWithSnapshotMode(재배포)만 남김
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | 다중 소스·다중 타깃 ① changelog 중립 계약: iceberg-sink
 * |                          | 인스턴스 이름을 소스별(dz-iceberg-<prefix>)로, route-regex를
 * |                          | 토픽 이름 정확 일치로 전환 (architecture.md 4·5.1절). 라우팅이
 * |                          | 토픽 기준으로 바뀌며 해소된 동명 테이블 제약(다른 스키마의
 * |                          | 동일 테이블명 거부)도 제거
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: 전역 topic-prefix 제거 — 커넥션별
 * |                          | topicPrefix로 소스마다 source·iceberg-sink를 독립 배포/해제
 * |                          | (deploySource로 단위화, ConnectorNames로 이름 조립). 딕셔너리를
 * |                          | DictionaryRouter로 교체(Oracle·PostgreSQL 분기, 8절). 등록 중복
 * |                          | 판정을 (source_connection_id, schema, table)로 전환. jdbc-sink
 * |                          | 타깃을 테이블별 targetConnectionId에서 조회하도록 수정(종전엔
 * |                          | 첫 등록 호출의 target 하나를 전체에 썼다)
 * |                          | iceberg-sink의 control 토픽을 소스별(control-iceberg-<prefix>)로
 * |                          | — 공유 control 토픽의 백로그 재생으로 커밋 응답이 늦어지는 실측 반영
 * --------------------------------------------------
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
    private final DictionaryRouter dictionaryRouter;
    private final ConnectorDeployService deploy;
    private final ChangelogTableService changelog;
    private final IcebergProperties iceberg;
    private final TableEventService events;
    private final String kafkaBootstrap;

    public RegistrationService(RegisteredTableRepository repository,
                               RegisteredColumnRepository columnRepository,
                               DbConnectionService connections,
                               DictionaryRouter dictionaryRouter,
                               ConnectorDeployService deploy,
                               ChangelogTableService changelog,
                               IcebergProperties iceberg,
                               TableEventService events,
                               @Value("${deltazium.kafka.bootstrap}") String kafkaBootstrap) {
        this.repository = repository;
        this.columnRepository = columnRepository;
        this.connections = connections;
        this.dictionaryRouter = dictionaryRouter;
        this.deploy = deploy;
        this.changelog = changelog;
        this.iceberg = iceberg;
        this.events = events;
        this.kafkaBootstrap = kafkaBootstrap;
    }

    public List<RegisteredTable> list() {
        return repository.findAll();
    }

    /** GET /api/registrations 전용 — 소스 topicPrefix를 얹은 뷰(UI 커넥터 이름 조립용). */
    public List<RegisteredTableView> listView() {
        return repository.findAll().stream()
                .map(t -> RegisteredTableView.of(t, connections.get(t.sourceConnectionId()).topicPrefix()))
                .toList();
    }

    public List<ColumnMapping> mappings(long registeredTableId) {
        return columnRepository.findByTable(registeredTableId);
    }

    /** 소스 딕셔너리에서 패턴에 걸리는 테이블 + 점검 상태 조회 (등록 후보). */
    public List<SourceTableInfo> discover(long sourceConnectionId, String pattern) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        return dictionaryRouter.forConnection(source).listTables(source, pattern);
    }

    /** 컬럼 목록 (소스/타깃 어느 연결이든) — 매핑 화면용. */
    public List<TableColumn> columns(long connectionId, String qualifiedTable) {
        int dot = qualifiedTable.indexOf('.');
        if (dot <= 0) {
            throw new IllegalArgumentException("SCHEMA.TABLE 형식이어야 한다: " + qualifiedTable);
        }
        DbConnection conn = connections.get(connectionId);
        return dictionaryRouter.forConnection(conn).listColumns(conn,
                qualifiedTable.substring(0, dot), qualifiedTable.substring(dot + 1));
    }

    /** DB 레벨 사전 점검 (예: Oracle ARCHIVELOG, PostgreSQL wal_level). */
    public List<PrecheckItem> databaseChecks(long sourceConnectionId) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        return dictionaryRouter.forConnection(source).databaseChecks(source);
    }

    /** 캡처 계정 권한 점검 — 누락 시 UI가 DBA용 GRANT 스크립트 안내 (자체 적용 불가). */
    public List<PrecheckItem> privilegeChecks(long sourceConnectionId) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        return dictionaryRouter.forConnection(source).privilegeChecks(source);
    }

    /** 캡처 사전조건 미리보기 — 승인 화면에 보여줄 DDL 문 (qualified → DDL). */
    public Map<String, String> captureSetupPreview(long sourceConnectionId, List<String> tables) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        return dictionaryRouter.forConnection(source).captureSetupPreview(source, tables);
    }

    /** 사용자가 UI에서 승인한 경우에만 호출 — 캡처 사전조건(supp.log/REPLICA IDENTITY FULL) 적용. */
    public Map<String, String> applyCaptureSetup(long sourceConnectionId, List<String> tables) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        return dictionaryRouter.forConnection(source).applyCaptureSetup(source, tables);
    }

    @Transactional
    public List<RegisteredTable> register(long sourceConnectionId, long targetConnectionId,
                                          List<TableSpec> specs) {
        return register(sourceConnectionId, targetConnectionId, specs, "INITIAL");
    }

    /**
     * @param snapshotMode INITIAL(초기적재 포함) | NO_DATA(현재 시점부터 변경만).
     *                     snapshot.mode는 커넥터 전역이라 실제 적용은 첫 등록(커넥터 생성) 시점 값 —
     *                     이후 등록의 값은 기록만 된다.
     */
    @Transactional
    public List<RegisteredTable> register(long sourceConnectionId, long targetConnectionId,
                                          List<TableSpec> specs, String snapshotMode) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        DbConnection target = requireRole(targetConnectionId, "TARGET");
        SourceDictionary dictionary = dictionaryRouter.forConnection(source);
        if (specs == null || specs.isEmpty()) {
            throw new IllegalArgumentException("등록할 테이블이 없다");
        }
        String mode = snapshotMode == null ? "INITIAL" : snapshotMode.toUpperCase(Locale.ROOT);
        if (!mode.equals("INITIAL") && !mode.equals("NO_DATA")) {
            throw new IllegalArgumentException("snapshotMode는 INITIAL 또는 NO_DATA여야 한다: " + snapshotMode);
        }

        // 검증 후 저장 — 하나라도 실패하면 전체 롤백
        for (TableSpec spec : specs) {
            SourceTableInfo info = validateTable(source, dictionary, spec.source());
            List<TableColumn> sourceColumns = dictionary.listColumns(source, info.schema(), info.table());
            List<ColumnMapping> mappings = normalizeMappings(spec, sourceColumns);

            long tableId = repository.insert(info.schema(), info.table(),
                    sourceConnectionId, targetConnectionId,
                    upperOrNull(spec.targetSchema()), upperOrNull(spec.targetTable()), mode);
            columnRepository.insertAll(tableId, mappings);
        }

        deployConnectors();
        for (TableSpec spec : specs) {
            int dot = spec.source().indexOf('.');
            events.info(spec.source().substring(0, dot).toUpperCase(Locale.ROOT),
                    spec.source().substring(dot + 1).toUpperCase(Locale.ROOT),
                    "REGISTERED", "CDC 등록·커넥터 배포 (source: " + source.name()
                            + " → target: " + target.name() + ")");
        }
        return repository.findAll();
    }

    private SourceTableInfo validateTable(DbConnection source, SourceDictionary dictionary, String qualified) {
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
        if (!info.captureReady()) {
            throw new IllegalArgumentException("캡처 사전조건 미충족 (" + dictionary.captureSetupLabel()
                    + "): " + qualified + " — 사전 점검 단계에서 적용 후 다시 시도");
        }
        if (repository.exists(source.id(), info.schema(), info.table())) {
            throw new IllegalArgumentException("이미 등록된 테이블: " + qualified);
        }
        // 등록 키는 (source_connection_id, schema, table) — 다른 소스의 동일 schema.table은
        // 별개 테이블이라 거부하지 않는다 (2026-09-07, 다중 소스). route-field도 토픽 이름
        // 기준(_pos.topic)이라 동명 테이블 라우팅 충돌도 없다 (2026-09-05, 5.1절).
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

    /**
     * 등록 전체 목록을 소스 커넥션별로 묶어 각자 배포 — 신규 등록·해제 후 갱신에 공용.
     * 소스 하나의 실패·설정 변경이 다른 소스에 영향을 주지 않는다(4절).
     */
    private void deployConnectors() {
        Map<Long, List<RegisteredTable>> bySource = repository.findAll().stream()
                .collect(Collectors.groupingBy(RegisteredTable::sourceConnectionId));
        for (Map.Entry<Long, List<RegisteredTable>> e : bySource.entrySet()) {
            deploySource(connections.get(e.getKey()), e.getValue(), null);
        }
        // 구버전 단일 jdbc-sink/전역 source가 남아있으면 제거 (테이블별·소스별 커넥터로 전환됨)
        try {
            deploy.deleteConnector("dz-jdbc-sink");
            deploy.deleteConnector("dz-source");
        } catch (Exception ignored) {
            // 없으면 그만 — 배포 흐름을 막지 않는다
        }
    }

    /**
     * 소스 커넥션 하나의 source·iceberg-sink(소스당 1개) + 그 소스에 속한 테이블들의
     * jdbc-sink(테이블당 1개)를 배포한다. 다른 소스는 건드리지 않는다.
     *
     * @param snapshotModeOverride null이면 그 소스의 첫 등록(가장 오래된 행) 선택을 따르고,
     *                             지정 시 그 모드로 배포(재스냅샷, ResnapshotOrchestrator 전용).
     */
    private void deploySource(DbConnection source, List<RegisteredTable> tables, String snapshotModeOverride) {
        String prefix = source.topicPrefix();
        String includeList = tables.stream().map(RegisteredTable::qualified)
                .collect(Collectors.joining(","));
        String topics = tables.stream()
                .map(t -> ConnectorNames.captureTopic(prefix, t.schemaName(), t.tableName()))
                .collect(Collectors.joining(","));

        for (RegisteredTable t : tables) {
            changelog.ensureChangelogTable(prefix, t.schemaName(), t.tableName());
        }

        String snapshotMode = snapshotModeOverride != null ? snapshotModeOverride
                : tables.stream()
                .min(Comparator.comparingLong(RegisteredTable::id))
                .map(t -> "NO_DATA".equalsIgnoreCase(t.snapshotMode()) ? "no_data" : "initial")
                .orElse("initial");

        deploySourceConnector(source, prefix, includeList, snapshotMode);

        // jdbc-sink: 테이블별 커넥터 — 타깃은 테이블별 targetConnectionId에서 조회한다
        // (종전엔 첫 등록 호출의 target 하나를 전체 재배포에 재사용했다 — 다중 타깃 전제로 수정).
        for (RegisteredTable t : tables) {
            DbConnection target = connections.get(t.targetConnectionId());
            List<ColumnMapping> mappings = columnRepository.findByTable(t.id());
            Map<String, String> vars = new HashMap<>();
            vars.put("connector_name", ConnectorNames.jdbcSink(prefix, t.suffix()));
            vars.put("topics", ConnectorNames.captureTopic(prefix, t.schemaName(), t.tableName()));
            vars.put("target_jdbc_url", target.jdbcUrl());
            vars.put("target_user", target.username());
            vars.put("target_password", target.password());
            vars.put("collection_name", t.targetQualified());
            deploy.deploy("jdbc-sink", vars, fieldIncludeConfig(mappings));
        }

        Map<String, String> icebergVars = new HashMap<>();
        icebergVars.put("connector_name", ConnectorNames.icebergSink(prefix));
        icebergVars.put("topics", topics);
        // control 토픽도 소스별 — 공유 시 새 인스턴스의 task가 다른 소스의 control 백로그를
        // 처음부터 읽느라 커밋 응답이 수 분 늦어진다 (2026-09-07 실측, connectors/README.md)
        icebergVars.put("topic_prefix", prefix);
        icebergVars.put("catalog_jdbc_url", iceberg.catalogUri());
        icebergVars.put("catalog_jdbc_user", iceberg.catalogUser());
        icebergVars.put("catalog_jdbc_password", iceberg.catalogPassword());
        icebergVars.put("warehouse", iceberg.warehouse());
        icebergVars.put("s3_endpoint", iceberg.s3Endpoint());
        icebergVars.put("s3_access_key", iceberg.s3AccessKey());
        icebergVars.put("s3_secret_key", iceberg.s3SecretKey());
        icebergVars.put("iceberg_tables", tables.stream()
                .map(t -> changelog.changelogTableName(prefix, t.schemaName(), t.tableName()))
                .collect(Collectors.joining(",")));
        Map<String, String> routeRegex = new HashMap<>();
        for (RegisteredTable t : tables) {
            // route-field=_pos.topic(템플릿) — 라우팅은 토픽 이름 정확 일치로,
            // 테이블명만 보던 종전 방식의 동명 테이블 제약을 해소 (5.1절)
            String topic = ConnectorNames.captureTopic(prefix, t.schemaName(), t.tableName());
            routeRegex.put("iceberg.table."
                    + changelog.changelogTableName(prefix, t.schemaName(), t.tableName()) + ".route-regex",
                    "^" + Pattern.quote(topic) + "$");
        }
        deploy.deploy("iceberg-sink", icebergVars, routeRegex);
    }

    /** source 커넥터 배포 — 템플릿·설정 키는 소스 dbType별로 다르다 (connectors/README.md). */
    private void deploySourceConnector(DbConnection source, String prefix, String includeList,
                                       String snapshotMode) {
        DbType type = DbType.find(source.dbType())
                .orElseThrow(() -> new IllegalArgumentException("알 수 없는 소스 DB 종류: " + source.dbType()));
        Map<String, String> vars = new HashMap<>();
        vars.put("connector_name", ConnectorNames.source(prefix));
        vars.put("topic_prefix", prefix);
        vars.put("table_include_list", includeList);
        vars.put("kafka_bootstrap", kafkaBootstrap);
        vars.put("snapshot_mode", snapshotMode);
        String template = switch (type) {
            case ORACLE -> {
                vars.put("oracle_host", source.host());
                vars.put("oracle_port", String.valueOf(source.port()));
                vars.put("oracle_user", source.username());
                vars.put("oracle_password", source.password());
                vars.put("oracle_dbname", source.databaseName());
                yield "source-oracle";
            }
            case POSTGRESQL -> {
                vars.put("pg_host", source.host());
                vars.put("pg_port", String.valueOf(source.port()));
                vars.put("pg_user", source.username());
                vars.put("pg_password", source.password());
                vars.put("pg_dbname", source.databaseName());
                vars.put("slot_name", "dz_" + prefix);
                vars.put("publication_name", "dz_" + prefix);
                yield "source-postgresql";
            }
            default -> throw new IllegalArgumentException("source 커넥터 템플릿 미정의: " + type.label());
        };
        deploy.deploy(template, vars);
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

    /**
     * 등록 전체 목록 기준 재배포 — 재스냅샷 오케스트레이터(ResnapshotOrchestrator ④단계) 전용.
     * 지정 소스 커넥션 하나만 배포하고 다른 소스는 건드리지 않는다(오케스트레이터가 단일 소스
     * 전제로 실행되므로 — 여러 소스가 섞인 재스냅샷은 지원하지 않는다, TODO ②).
     */
    public void redeployWithSnapshotMode(long sourceConnectionId, String snapshotMode) {
        List<RegisteredTable> tables = repository.findBySource(sourceConnectionId);
        if (tables.isEmpty()) {
            throw new IllegalStateException("해당 소스에 등록된 테이블이 없다: id=" + sourceConnectionId);
        }
        deploySource(connections.get(sourceConnectionId), tables, snapshotMode);
    }

    /** 일시 정지 — 해당 테이블 apply만 멈춘다. 캡처·changelog 축적은 계속(재개 시 캐치업). */
    public void pause(long registeredTableId) {
        RegisteredTable t = find(registeredTableId);
        String prefix = connections.get(t.sourceConnectionId()).topicPrefix();
        deploy.pauseConnector(ConnectorNames.jdbcSink(prefix, t.suffix()));
        events.info(t.schemaName(), t.tableName(), "PAUSED",
                "apply 정지 — 캡처·changelog 축적은 계속");
    }

    public void resume(long registeredTableId) {
        RegisteredTable t = find(registeredTableId);
        String prefix = connections.get(t.sourceConnectionId()).topicPrefix();
        deploy.resumeConnector(ConnectorNames.jdbcSink(prefix, t.suffix()));
        events.info(t.schemaName(), t.tableName(), "RESUMED", "apply 재개 — 밀린 분부터 캐치업");
    }

    /**
     * 등록 해제 — 커넥터에서 제거 + 메타데이터 삭제.
     * 해제된 테이블이 그 소스의 마지막 테이블이면 그 소스의 source·iceberg-sink만 정리한다
     * (다른 소스 무영향, TODO ②). 남은 테이블이 있으면 그 소스만 재배포.
     * @param dropChangelog true면 changelog(Iceberg/S3) 데이터까지 삭제 —
     *                      복구 원본이 사라지므로 UI에서 명시 확인을 받은 값이어야 한다. 기본 보존.
     */
    @Transactional
    public List<RegisteredTable> unregister(long registeredTableId, boolean dropChangelog) {
        RegisteredTable table = find(registeredTableId);
        DbConnection source = connections.get(table.sourceConnectionId());
        String prefix = source.topicPrefix();
        columnRepository.deleteByTable(registeredTableId);
        repository.delete(registeredTableId);

        // 테이블별 sink 제거 (recovery-sink는 있을 때만)
        quietDelete(ConnectorNames.jdbcSink(prefix, table.suffix()));
        quietDelete(ConnectorNames.recoverySink(prefix, table.suffix()));

        List<RegisteredTable> remaining = repository.findAll();
        List<RegisteredTable> remainingOfSource = remaining.stream()
                .filter(t -> t.sourceConnectionId() == table.sourceConnectionId()).toList();
        if (remainingOfSource.isEmpty()) {
            // 이 소스의 마지막 테이블 — offset 정리 후(커넥터가 STOPPED로 존재해야 삭제 가능,
            // 같은 이름 재등록 시 스냅샷 SKIP 방지) 이 소스의 source·iceberg-sink만 정리
            resetConnectorOffsets(ConnectorNames.source(prefix));
            quietDelete(ConnectorNames.source(prefix));
            quietDelete(ConnectorNames.icebergSink(prefix));
        } else {
            deploySource(source, remainingOfSource, null);
        }

        if (dropChangelog) {
            changelog.dropChangelogTable(prefix, table.schemaName(), table.tableName(), true);
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

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
import io.deltazium.backend.ddl.SchemaFingerprint;
import io.deltazium.backend.ddl.TargetDdlExecutor;
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
import io.deltazium.backend.registry.PostgresReplicationCleaner;
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
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ③ 저장소 프로파일(MinIO/R2): iceberg-sink
 * |                          | 배포에서 catalog_jdbc_url 등 개별 vars를 제거하고
 * |                          | `IcebergProperties.catalogProperties()`를 `iceberg.catalog.<key>`
 * |                          | 접두로 extraConfig에 병합(템플릿의 catalog 블록 제거와 대응,
 * |                          | connectors/README.md)
 * --------------------------------------------------
 * 26. 09. 22.       | 최남희  | PG 타깃 검증(2026-09-22)에서 발견한 결함 2건 수정.
 * |                          | D1: 타깃 스키마·테이블명 저장을 upperOrNull(무조건 대문자)에서
 * |                          | 타깃 커넥션 DbType.foldIdentifier로 전환(Oracle 대문자·PG
 * |                          | 소문자) — PG 타깃 DDL 승인이 존재하지 않는 대문자 스키마를
 * |                          | 찾아 502로 실패하던 결함(architecture.md 8절). 기존 등록 행은
 * |                          | 마이그레이션하지 않음(재등록 절차, docs/operations.md).
 * |                          | D2: REGISTERED 이벤트 키를 소스 원문(딕셔너리 조회 결과)으로
 * |                          | 통일 — 기존엔 대문자로 저장돼 DdlEventService 등 다른 기록처와
 * |                          | 달라 테이블 drawer 이벤트 목록에서 누락됐다.
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 결함 수정: PG 소스의 마지막 테이블 해제(source 커넥터 삭제) 직후
 * |                          | PostgresReplicationCleaner로 복제 슬롯·publication 정리 —
 * |                          | inactive 슬롯이 남아 WAL 정리를 막던 문제(2026-09-22~23 관측).
 * |                          | 슬롯·publication 이름 리터럴("dz_"+prefix)을
 * |                          | ConnectorNames.replicationSlot/Publication으로 단일화
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | DDL 반영 정책(ddlPolicy)·타깃 테이블 생성 옵션(createTarget)
 * |                          | 추가(architecture.md 7·8절). TableSpec에 두 필드 추가(기존 4-arg
 * |                          | 생성자는 MANUAL·false로 위임). createTarget이면 소스 스키마로
 * |                          | CREATE TABLE을 등록 트랜잭션 안에서 실행(previewTargetTableDdl과
 * |                          | 같은 함수로 조립 — 클라이언트가 보낸 DDL 문자열은 신뢰하지 않는다)
 * |                          | 하고 컬럼 매핑은 동일명 전부 활성으로 고정한다. DDL 건너뛰기
 * |                          | (SKIPPED, DdlEventService 전용)를 위해 redeployJdbcSinkOnly·
 * |                          | addDisabledColumn·disableColumn 추가 — 소스 전체 재배포
 * |                          | (deploySource)는 다른 테이블까지 건드려 과도하므로 테이블 하나의
 * |                          | jdbc-sink만 다시 배포한다
 * --------------------------------------------------
 */
@Service
public class RegistrationService {

    /**
     * 등록 요청의 테이블 한 건. target·columns가 비면 소스와 동일/전 컬럼으로 저장한다.
     * @param createTarget true면 소스 스키마로 타깃 테이블을 새로 생성한다(8절) — 이미 존재하면
     *                      등록 거부. 이때 columns는 무시되고 동일명 전부 활성으로 고정된다.
     * @param ddlPolicy MANUAL(기본, 확인 후 반영) | AUTO(감지 즉시 적용 후 재개, 7절).
     */
    public record TableSpec(String source, String targetSchema, String targetTable,
                            List<ColumnMapping> columns, boolean createTarget, String ddlPolicy) {

        public TableSpec {
            ddlPolicy = normalizeDdlPolicy(ddlPolicy);
        }

        public TableSpec(String source, String targetSchema, String targetTable, List<ColumnMapping> columns) {
            this(source, targetSchema, targetTable, columns, false, "MANUAL");
        }

        private static String normalizeDdlPolicy(String raw) {
            if (raw == null || raw.isBlank()) {
                return "MANUAL";
            }
            String up = raw.toUpperCase(Locale.ROOT);
            if (!up.equals("MANUAL") && !up.equals("AUTO")) {
                throw new IllegalArgumentException("ddlPolicy는 MANUAL 또는 AUTO여야 한다: " + raw);
            }
            return up;
        }
    }

    private final RegisteredTableRepository repository;
    private final RegisteredColumnRepository columnRepository;
    private final DbConnectionService connections;
    private final DictionaryRouter dictionaryRouter;
    private final ConnectorDeployService deploy;
    private final ChangelogTableService changelog;
    private final IcebergProperties iceberg;
    private final TableEventService events;
    private final PostgresReplicationCleaner replicationCleaner;
    private final TargetDdlExecutor targetDdlExecutor;
    private final String kafkaBootstrap;

    public RegistrationService(RegisteredTableRepository repository,
                               RegisteredColumnRepository columnRepository,
                               DbConnectionService connections,
                               DictionaryRouter dictionaryRouter,
                               ConnectorDeployService deploy,
                               ChangelogTableService changelog,
                               IcebergProperties iceberg,
                               TableEventService events,
                               PostgresReplicationCleaner replicationCleaner,
                               TargetDdlExecutor targetDdlExecutor,
                               @Value("${deltazium.kafka.bootstrap}") String kafkaBootstrap) {
        this.repository = repository;
        this.columnRepository = columnRepository;
        this.connections = connections;
        this.dictionaryRouter = dictionaryRouter;
        this.deploy = deploy;
        this.changelog = changelog;
        this.iceberg = iceberg;
        this.events = events;
        this.replicationCleaner = replicationCleaner;
        this.targetDdlExecutor = targetDdlExecutor;
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
        DbType sourceType = DbType.find(source.dbType())
                .orElseThrow(() -> new IllegalArgumentException("알 수 없는 소스 DB 종류: " + source.dbType()));
        DbType targetType = DbType.find(target.dbType())
                .orElseThrow(() -> new IllegalArgumentException("알 수 없는 타깃 DB 종류: " + target.dbType()));
        if (specs == null || specs.isEmpty()) {
            throw new IllegalArgumentException("등록할 테이블이 없다");
        }
        String mode = snapshotMode == null ? "INITIAL" : snapshotMode.toUpperCase(Locale.ROOT);
        if (!mode.equals("INITIAL") && !mode.equals("NO_DATA")) {
            throw new IllegalArgumentException("snapshotMode는 INITIAL 또는 NO_DATA여야 한다: " + snapshotMode);
        }

        // 검증 후 저장 — 하나라도 실패하면 전체 롤백
        List<SourceTableInfo> infos = new ArrayList<>();
        for (TableSpec spec : specs) {
            SourceTableInfo info = validateTable(source, dictionary, spec.source());
            infos.add(info);
            List<TableColumn> sourceColumns = dictionary.listColumns(source, info.schema(), info.table());
            String foldedTargetSchema = foldTargetIdentifier(targetType, spec.targetSchema(), info.schema());
            String foldedTargetTable = foldTargetIdentifier(targetType, spec.targetTable(), info.table());

            List<ColumnMapping> mappings;
            if (spec.createTarget()) {
                createTargetTable(target, sourceType, targetType, sourceColumns,
                        foldedTargetSchema, foldedTargetTable);
                mappings = defaultMappings(sourceColumns);
            } else {
                mappings = normalizeMappings(spec, sourceColumns);
            }

            long tableId = repository.insert(info.schema(), info.table(),
                    sourceConnectionId, targetConnectionId,
                    foldedTargetSchema, foldedTargetTable, mode, spec.ddlPolicy());
            columnRepository.insertAll(tableId, mappings);
        }

        deployConnectors();
        for (SourceTableInfo info : infos) {
            // 원문(딕셔너리 조회 결과) 그대로 — DdlEventService·커넥터 상태 모니터 등 다른
            // 기록처와 같은 키를 쓰기 위함(대문자로 바꾸면 UI 정확 일치 필터에서 누락된다, D2)
            events.info(info.schema(), info.table(),
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
            return defaultMappings(sourceColumns);
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

    /** 전 컬럼 동일명 매핑(전부 활성) — 매핑 미지정 시 기본값이자, "소스 스키마로 새로 생성"
     * (createTarget) 옵션의 고정 매핑(8절: "매핑은 동일명 전부 활성으로 고정"). */
    private static List<ColumnMapping> defaultMappings(List<TableColumn> sourceColumns) {
        return sourceColumns.stream()
                .map(c -> new ColumnMapping(c.name(), "${" + c.name() + "}", true))
                .toList();
    }

    /**
     * 등록 시 "소스 스키마로 새로 생성" 옵션(8절) — 타깃에 이미 같은 이름의 테이블이 있으면
     * 등록을 거부한다(존재 여부는 딕셔너리 컬럼 조회로 판정 — 컬럼이 하나도 안 잡히면 없는
     * 것으로 본다, 위저드의 "타깃 컬럼 불러오기"와 같은 방식). DDL은 previewTargetTableDdl과
     * 같은 함수(SchemaFingerprint.draftCreateTable)로 조립한다 — 클라이언트가 보낸 DDL 문자열은
     * 신뢰하지 않고 서버가 유일한 진원지가 되도록 한다.
     */
    private void createTargetTable(DbConnection target, DbType sourceType, DbType targetType,
                                   List<TableColumn> sourceColumns, String targetSchema, String targetTable) {
        boolean exists = !dictionaryRouter.forConnection(target)
                .listColumns(target, targetSchema, targetTable).isEmpty();
        if (exists) {
            throw new IllegalArgumentException("타깃 테이블이 이미 존재한다: " + targetSchema + "." + targetTable);
        }
        String ddl = SchemaFingerprint.draftCreateTable(sourceType, targetType, targetSchema, targetTable,
                sourceColumns);
        targetDdlExecutor.execute(target, ddl);
    }

    /**
     * "소스 스키마로 새로 생성" 미리보기 — 등록 전 화면에 보여줄 CREATE TABLE 초안
     * (POST /api/registrations/target-table/preview). createTargetTable과 같은 함수로 조립해
     * 미리보기와 실제 생성이 항상 일치한다. 매핑 불가 타입이 있으면 거부(사유 포함) —
     * SchemaFingerprint.draftCreateTable이 던진다.
     */
    public String previewTargetTableDdl(long sourceConnectionId, String sourceQualifiedTable,
                                        long targetConnectionId, String targetSchema, String targetTable) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        DbConnection target = requireRole(targetConnectionId, "TARGET");
        DbType sourceType = DbType.find(source.dbType())
                .orElseThrow(() -> new IllegalArgumentException("알 수 없는 소스 DB 종류: " + source.dbType()));
        DbType targetType = DbType.find(target.dbType())
                .orElseThrow(() -> new IllegalArgumentException("알 수 없는 타깃 DB 종류: " + target.dbType()));
        int dot = sourceQualifiedTable == null ? -1 : sourceQualifiedTable.indexOf('.');
        if (dot <= 0) {
            throw new IllegalArgumentException("SCHEMA.TABLE 형식이어야 한다: " + sourceQualifiedTable);
        }
        String schema = sourceQualifiedTable.substring(0, dot);
        String table = sourceQualifiedTable.substring(dot + 1);
        List<TableColumn> sourceColumns = dictionaryRouter.forConnection(source).listColumns(source, schema, table);
        if (sourceColumns.isEmpty()) {
            throw new IllegalArgumentException("소스 컬럼을 찾을 수 없다: " + sourceQualifiedTable);
        }
        String foldedSchema = foldTargetIdentifier(targetType, targetSchema, schema);
        String foldedTable = foldTargetIdentifier(targetType, targetTable, table);
        return SchemaFingerprint.draftCreateTable(sourceType, targetType, foldedSchema, foldedTable, sourceColumns);
    }

    /**
     * 타깃 스키마·테이블명 저장값 — 명시 값이 없으면 소스 이름(원문)으로 대체한 뒤,
     * 타깃 DbType의 폴딩 규칙(DbType.foldIdentifier, 8절)을 적용한다. 이 저장값 하나가
     * registered_tables·jdbc-sink collection.name.format·DDL 승인 초안(SchemaFingerprint)·
     * DDL 치환(DdlEventService.rewriteForTarget)에 공유된다 — 한 곳에서만 폴딩한다.
     */
    private static String foldTargetIdentifier(DbType targetType, String explicit, String sourceFallback) {
        String raw = explicit == null || explicit.isBlank() ? sourceFallback : explicit.trim();
        return targetType.foldIdentifier(raw);
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
        icebergVars.put("iceberg_tables", tables.stream()
                .map(t -> changelog.changelogTableName(prefix, t.schemaName(), t.tableName()))
                .collect(Collectors.joining(",")));

        // 카탈로그 접속 정보는 커넥터 정의가 아니라 설치 프로파일에 속한다(3절) — 템플릿에서
        // 빠진 iceberg.catalog.* 블록을 여기서 extraConfig로 채운다 (TODO ③, 단일 진원지)
        Map<String, String> extraConfig = new HashMap<>();
        iceberg.catalogProperties().forEach((k, v) -> extraConfig.put("iceberg.catalog." + k, v));
        for (RegisteredTable t : tables) {
            // route-field=_pos.topic(템플릿) — 라우팅은 토픽 이름 정확 일치로,
            // 테이블명만 보던 종전 방식의 동명 테이블 제약을 해소 (5.1절)
            String topic = ConnectorNames.captureTopic(prefix, t.schemaName(), t.tableName());
            extraConfig.put("iceberg.table."
                    + changelog.changelogTableName(prefix, t.schemaName(), t.tableName()) + ".route-regex",
                    "^" + Pattern.quote(topic) + "$");
        }
        deploy.deploy("iceberg-sink", icebergVars, extraConfig);
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
                vars.put("slot_name", ConnectorNames.replicationSlot(prefix));
                vars.put("publication_name", ConnectorNames.replicationPublication(prefix));
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
            if (DbType.find(source.dbType()).orElse(null) == DbType.POSTGRESQL) {
                cleanupPostgresReplication(source, prefix, table);
            }
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

    /**
     * PostgreSQL 소스의 마지막 테이블 해제 직후 슬롯·publication 정리 (결함 2 수정,
     * 2026-09-22~23 관측 — inactive 슬롯이 남아 WAL 정리를 막던 문제, docs/internals.md
     * "PG 복제 슬롯 정리" 절). 실패해도 등록 해제 자체는 실패시키지 않는다 — WARN 이벤트로
     * 남기고 사용자가 이벤트 메시지의 SQL로 수동 정리할 수 있게 한다.
     */
    private void cleanupPostgresReplication(DbConnection source, String prefix, RegisteredTable table) {
        String slot = ConnectorNames.replicationSlot(prefix);
        String publication = ConnectorNames.replicationPublication(prefix);
        String manualSql = "SELECT pg_drop_replication_slot('" + slot + "'); "
                + "DROP PUBLICATION IF EXISTS \"" + publication + "\";";
        try {
            PostgresReplicationCleaner.Result result = replicationCleaner.cleanup(source, slot, publication);
            if (result.slotDropped() && result.publicationDropped()) {
                events.info(table.schemaName(), table.tableName(), "SLOT_DROPPED",
                        "PG 복제 슬롯·publication 정리 완료 (" + slot + ")");
            } else {
                events.record(table.schemaName(), table.tableName(), "SLOT_CLEANUP_FAILED", "WARN",
                        "PG 복제 슬롯이 아직 active라 정리하지 못함 — 수동 정리: " + manualSql, null);
            }
        } catch (Exception e) {
            events.record(table.schemaName(), table.tableName(), "SLOT_CLEANUP_FAILED", "WARN",
                    "PG 복제 슬롯·publication 정리 실패(" + e.getMessage() + ") — 수동 정리: " + manualSql, null);
        }
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

    /**
     * 해당 테이블의 jdbc-sink만 다시 배포한다 — DDL 건너뛰기(SKIPPED, DdlEventService 전용)로
     * 컬럼 매핑이 바뀐 뒤 반영에 쓴다. deploySource()는 그 소스의 source·iceberg-sink와 다른
     * 테이블들의 jdbc-sink까지 전부 재배포해 이 용도엔 과도하다(다른 테이블 무영향 원칙).
     */
    public void redeployJdbcSinkOnly(long registeredTableId) {
        RegisteredTable t = find(registeredTableId);
        String prefix = connections.get(t.sourceConnectionId()).topicPrefix();
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

    /**
     * DDL 건너뛰기(SKIPPED) — ADD COLUMN: 새 컬럼을 비활성 매핑으로 추가하고 재배포한다.
     * jdbc-sink field.include.list에서 빠져 타깃엔 반영되지 않지만, changelog(Iceberg)는
     * 스키마 진화를 자동 수용해 그대로 받으므로 나중에 활성화하면 소급 없이 반영을 시작할 수
     * 있다(architecture.md 7절 SKIPPED 의미, docs/internals.md). 이미 매핑돼 있으면 그대로 둔다
     * (멱등 — 같은 이벤트를 다시 건너뛰어도 안전).
     */
    public void addDisabledColumn(long registeredTableId, String columnName) {
        RegisteredTable t = find(registeredTableId);
        List<ColumnMapping> existing = columnRepository.findByTable(t.id());
        boolean already = existing.stream().anyMatch(m -> m.targetColumn().equalsIgnoreCase(columnName));
        if (!already) {
            columnRepository.insertAll(t.id(), List.of(new ColumnMapping(columnName, "${" + columnName + "}", false)));
        }
        redeployJdbcSinkOnly(registeredTableId);
    }

    /**
     * DDL 건너뛰기(SKIPPED) — DROP COLUMN: 소스에서 사라진 컬럼의 매핑을 비활성화하고
     * 재배포한다. 타깃 컬럼 자체는 지우지 않는다(7절: "타깃 컬럼은 남긴다").
     */
    public void disableColumn(long registeredTableId, String columnName) {
        RegisteredTable t = find(registeredTableId);
        columnRepository.updateEnabled(t.id(), columnName, false);
        redeployJdbcSinkOnly(registeredTableId);
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

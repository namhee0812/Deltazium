/**
 * 파일명 : types.ts
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : DB 연결 도메인 타입 정의.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: topicPrefix 추가(SOURCE 전용, 소스 식별자)
 * --------------------------------------------------
 * 26. 09. 28.       | 최남희  | SingleStore(타깃 전용) 지원 — DbTypeOption에 sourceCapable 추가
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | Databricks(타깃, DW) 지원 — httpPath·stagingVolume 필드 추가
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | DwApplyInfo.status에 skippedRecords 추가
 * --------------------------------------------------
 * 26. 10. 20.       | 최남희  | DbTypeOption에 dwTarget 추가
 * --------------------------------------------------
 */
export interface DbConnection {
  id: number | null
  name: string
  dbType: string
  role: 'SOURCE' | 'TARGET'
  host: string
  port: number
  databaseName: string
  username: string
  password?: string
  /** SOURCE 전용 — Debezium topic.prefix(소스 식별자). TARGET은 null. */
  topicPrefix?: string | null
  /** DATABRICKS 전용 — SQL warehouse HTTP path. */
  httpPath?: string | null
  /** DATABRICKS 전용 — 스테이징 Volume `<schema>.<volume>` (카탈로그=databaseName 안). */
  stagingVolume?: string | null
}

/** GET /api/dw-apply 한 건 — DW 타깃(Databricks) 적재 워커 상태. */
export interface DwApplyInfo {
  targetId: number
  targetName: string
  running: boolean
  error: string | null
  status: {
    lastBatchAt?: string | null
    lastError?: string | null
    skippedRecords?: number
  } | null
}

export interface DbTypeOption {
  code: string
  label: string
  /** false면 SOURCE 역할로 선택할 수 없다(예: SingleStore — 타깃 전용, architecture.md 8절). */
  sourceCapable: boolean
  /** true면 JDBC sink가 아니라 DW apply 워커가 적재하는 타깃(Databricks 등). */
  dwTarget?: boolean
}

export interface TestResult {
  ok: boolean
  message: string
}

export const emptyConnection: DbConnection = {
  id: null,
  name: '',
  dbType: 'ORACLE',
  role: 'SOURCE',
  host: '',
  port: 1521,
  databaseName: '',
  username: '',
  password: '',
}

/**
 * mock 데이터 — backend에 테이블 등록·DDL 이벤트 API가 생기면 실데이터로 교체한다.
 * (마일스톤 4: 테이블 등록, 마일스톤 6: DDL 승인 워크플로)
 */

export interface CdcTable {
  id: number
  schema: string
  name: string
  targets: string[]
  suppLog: 'full' | 'pk-only' | 'none'
  status: 'ok' | 'warn' | 'crit'
}

export const mockTables: CdcTable[] = [
  { id: 1, schema: 'SRC', name: 'ORDERS', targets: ['Oracle TGT', 'Iceberg'], suppLog: 'full', status: 'ok' },
  { id: 2, schema: 'SRC', name: 'ORDER_ITEMS', targets: ['Oracle TGT', 'Iceberg'], suppLog: 'full', status: 'ok' },
  { id: 3, schema: 'SRC', name: 'CUSTOMERS', targets: ['Oracle TGT', 'Iceberg'], suppLog: 'pk-only', status: 'ok' },
  { id: 4, schema: 'SRC', name: 'EMPLOYEES', targets: ['Oracle TGT', 'Iceberg'], suppLog: 'full', status: 'warn' },
  { id: 5, schema: 'SRC', name: 'PAYROLL_HISTORY', targets: ['Oracle TGT', 'Iceberg'], suppLog: 'none', status: 'crit' },
  { id: 6, schema: 'SRC', name: 'STOCK_MOVEMENTS', targets: ['Oracle TGT', 'Iceberg'], suppLog: 'full', status: 'ok' },
]

export interface TableMetrics {
  dml: number[]
  lag: number[]
  ins: number
  upd: number
  del: number
  scn: string
}

export function genMetrics(t: CdcTable): TableMetrics {
  return {
    dml: Array.from({ length: 24 }, () => 50 + Math.random() * 500),
    lag: Array.from({ length: 24 }, () =>
      t.status === 'ok' ? 20 + Math.random() * 80 : 300 + Math.random() * 900,
    ),
    ins: Math.floor(Math.random() * 900),
    upd: Math.floor(Math.random() * 400),
    del: Math.floor(Math.random() * 60),
    scn: String(48210000000 + Math.floor(Math.random() * 9e6)),
  }
}

export function tickMetrics(t: CdcTable, m: TableMetrics): TableMetrics {
  return {
    ...m,
    dml: [...m.dml.slice(1), 50 + Math.random() * 500],
    lag: [
      ...m.lag.slice(1),
      t.status === 'ok' ? 20 + Math.random() * 80 : 300 + Math.random() * 900,
    ],
    scn: String(BigInt(m.scn) + BigInt(Math.floor(Math.random() * 40000))),
  }
}

export interface DdlEvent {
  id: number
  ts: string
  scn: string
  table: string
  op: string
  sql: string
  state: 'applied' | 'pending' | 'blocked' | 'skipped'
  note: string
}

export const mockDdlEvents: DdlEvent[] = [
  {
    id: 1, ts: '2026-07-24 09:41:22', scn: '48219442810', table: 'SRC.ORDERS', op: 'ADD COLUMN',
    sql: 'ALTER TABLE SRC.ORDERS ADD (PROMO_CODE VARCHAR2(24))',
    state: 'applied', note: '타깃 Oracle에 승인 후 반영 — jdbc-sink 재개됨',
  },
  {
    id: 2, ts: '2026-07-23 22:03:57', scn: '48214980112', table: 'SRC.EMPLOYEES', op: 'MODIFY COLUMN',
    sql: 'ALTER TABLE SRC.EMPLOYEES MODIFY (EMAIL VARCHAR2(320))',
    state: 'applied', note: '길이 확장 — 자동 승인 후보 (ADD/확장 계열)',
  },
  {
    id: 3, ts: '2026-07-22 14:18:03', scn: '48198231544', table: 'SRC.PAYROLL_HISTORY', op: 'ADD INDEX',
    sql: 'CREATE INDEX SRC.IDX_PAYROLL_YM ON SRC.PAYROLL_HISTORY(PAY_MONTH)',
    state: 'skipped', note: '인덱스 DDL은 타깃 미전파 (Iceberg changelog에는 영향 없음)',
  },
  {
    id: 4, ts: '2026-07-21 10:52:41', scn: '48176003921', table: 'SRC.CUSTOMERS', op: 'DROP COLUMN',
    sql: 'ALTER TABLE SRC.CUSTOMERS DROP (FAX_NUMBER)',
    state: 'pending', note: '파괴적 변경 — 승인 대기. 대기 중에도 changelog는 Kafka·Iceberg에 계속 축적',
  },
  {
    id: 5, ts: '2026-07-20 16:29:10', scn: '48140887265', table: 'SRC.STOCK_MOVEMENTS', op: 'TRUNCATE',
    sql: 'TRUNCATE TABLE SRC.STOCK_MOVEMENTS',
    state: 'blocked', note: '거부됨 — jdbc-sink에서 해당 테이블 토픽 제외. 재개 시 6.3절 재발행 경로로 캐치업',
  },
]

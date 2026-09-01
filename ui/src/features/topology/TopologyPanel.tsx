/**
 * 파일명 : TopologyPanel.tsx
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : 대시보드 — KPI 카드(커넥터/처리량/최대 lag/미승인 DDL) + 토폴로지(자체 SVG) +
 * 주의 필요 목록 + 처리량·lag 시계열(테이블·기간 선택) + 최근 이벤트 + 컴포넌트 자원.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 상태 판정을 effectiveState(connector+task 최악값)로 교체
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 대시보드로 확장 — 차트·자원·이벤트 추가
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | React Flow → 자체 SVG(교차 없는 직교 라우팅, 사라짐 이슈 제거),
 * |                          | 테이블 검색 콤보박스·주기(1분/1시간/1일) 선택 추가
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 주기 버튼을 기간 중심(최근 6시간/7일/90일)으로, 해상도는 툴팁 —
 * |                          | 보존·롤업 내부 설명 텍스트는 UI에서 제거 (문서 몫)
 * --------------------------------------------------
 * 26. 08. 28.       | 최남희  | 리디자인 — KPI 카드 4개(커넥터·처리량·최대 lag·미승인 DDL) +
 * |                          | "주의 필요" 카드(lag 초과·DDL 대기, 탭 이동) 추가. 카드 프리미티브로
 * |                          | 전면 재구성, 신규 backend API 없이 기존 /api/metrics/tables·
 * |                          | /api/ddl-events를 이 화면에서도 폴링해 KPI를 구성한다.
 * --------------------------------------------------
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { ArrowRight } from 'lucide-react'
import { api } from '@/lib/api'
import { effectiveState } from '@/lib/connect'
import type { ConnectorStates } from '@/lib/connect'
import type { DbConnection } from '@/features/connections/types'
import { CHART_SERIES_COLORS, LineChart } from '@/components/LineChart'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { GhostButton } from '@/components/ui/ghost-button'
import { Segmented } from '@/components/ui/segmented'
import { StatusPill } from '@/components/ui/status-pill'
import type { StatusPillVariant } from '@/components/ui/status-pill'
import { TopologySvg } from './TopologySvg'
import type { NodeStatus, TopoData } from './TopologySvg'

interface Dashboard {
  throughput: { ts: string; publish: number; apply: number }[]
  lag: { ts: string; jdbc: number; iceberg: number }[]
  resources: { component: string; cpuPct: number; rssMb: number }[]
}

interface Ev {
  occurredAt: string
  eventType: string
  severity: string
  message: string
}

interface RegisteredTable {
  schemaName: string
  tableName: string
}

interface TableMetrics {
  schemaName: string
  tableName: string
  jdbcLag: number
}

interface DdlEvent {
  eventTsMs: number
  schemaName: string | null
  tableName: string | null
  ddlText: string
  state: 'SNAPSHOT' | 'DETECTED' | 'APPROVED' | 'REJECTED' | 'IGNORED'
}

/** JDBC lag 경고 임계(레코드 건수) — 테이블 모니터링 화면과 동일 기준 */
const LAG_WARN = 100

/** 주기 선택 — 해상도와 조회 폭을 묶는다 (Grafana식 기간별 자동 해상도) */
const PERIODS = [
  { key: 'MIN', label: '최근 6시간', title: '1분 단위', hours: 6 },
  { key: 'HOUR', label: '최근 7일', title: '1시간 단위', hours: 168 },
  { key: 'DAY', label: '최근 90일', title: '1일 단위', hours: 2160 },
] as const

const STATUS_TO_PILL: Record<NodeStatus, StatusPillVariant> = {
  ok: 'ok',
  warn: 'warn',
  crit: 'crit',
  none: 'stop',
}

function relativeTime(ms: number): string {
  const diffMin = Math.floor((Date.now() - ms) / 60000)
  if (diffMin < 1) return '방금 전'
  if (diffMin < 60) return `${diffMin}분 전`
  const diffHour = Math.floor(diffMin / 60)
  if (diffHour < 24) return `${diffHour}시간 전`
  return `${Math.floor(diffHour / 24)}일 전`
}

function connectorStatus(states: ConnectorStates | null, name: string): NodeStatus {
  if (!states || !(name in states)) return 'none'
  const s = effectiveState(states[name])
  return s === 'RUNNING' ? 'ok' : s === 'PAUSED' ? 'warn' : 'crit'
}

function jdbcSinkAggregate(states: ConnectorStates | null): { status: NodeStatus; count: number } {
  if (!states) return { status: 'none', count: 0 }
  const sinks = Object.entries(states).filter(([n]) => n.startsWith('dz-jdbc-sink-'))
  if (sinks.length === 0) return { status: 'none', count: 0 }
  const st = sinks.map(([, i]) => effectiveState(i))
  const status: NodeStatus = st.some((s) => s !== 'RUNNING' && s !== 'PAUSED')
    ? 'crit'
    : st.some((s) => s === 'PAUSED')
      ? 'warn'
      : 'ok'
  return { status, count: sinks.length }
}

export function TopologyPanel({
  onNavigate,
}: {
  /** "보기/검토" 액션 · KPI 링크 클릭 시 해당 탭으로 이동 (App이 소유한 탭 상태를 바꾼다) */
  onNavigate?: (view: 'tables' | 'ddl') => void
}) {
  const [connectors, setConnectors] = useState<ConnectorStates | null>(null)
  const [connections, setConnections] = useState<DbConnection[]>([])
  const [dashboard, setDashboard] = useState<Dashboard | null>(null)
  const [events, setEvents] = useState<Ev[]>([])
  const [tables, setTables] = useState<RegisteredTable[]>([])
  const [tableMetrics, setTableMetrics] = useState<TableMetrics[] | null>(null)
  const [ddlEvents, setDdlEvents] = useState<DdlEvent[] | null>(null)
  const [period, setPeriod] = useState<(typeof PERIODS)[number]>(PERIODS[0])
  const [table, setTable] = useState<string>('all') // 'all' 또는 topic
  const [tableQuery, setTableQuery] = useState('')
  const [tableOpen, setTableOpen] = useState(false)
  const comboRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const load = () => {
      api<ConnectorStates>('/api/connectors').then(setConnectors).catch(() => setConnectors(null))
      api<DbConnection[]>('/api/connections').then(setConnections).catch(() => setConnections([]))
      api<TableMetrics[]>('/api/metrics/tables').then(setTableMetrics).catch(() => setTableMetrics(null))
      api<DdlEvent[]>('/api/ddl-events').then(setDdlEvents).catch(() => setDdlEvents(null))
    }
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [])

  useEffect(() => {
    api<RegisteredTable[]>('/api/registrations').then(setTables).catch(() => {})
  }, [])

  useEffect(() => {
    const load = () => {
      api<Dashboard>(
        `/api/metrics/dashboard?res=${period.key}&hours=${period.hours}&table=${encodeURIComponent(table)}`)
        .then(setDashboard)
        .catch(() => {})
      api<Ev[]>('/api/events?limit=6').then(setEvents).catch(() => {})
    }
    load()
    const id = setInterval(load, 30000)
    return () => clearInterval(id)
  }, [period, table])

  // 콤보박스 바깥 클릭 시 닫기
  useEffect(() => {
    const close = (e: MouseEvent) => {
      if (comboRef.current && !comboRef.current.contains(e.target as Node)) setTableOpen(false)
    }
    document.addEventListener('mousedown', close)
    return () => document.removeEventListener('mousedown', close)
  }, [])

  const topo: TopoData = useMemo(() => {
    const source = connections.find((c) => c.role === 'SOURCE')
    const target = connections.find((c) => c.role === 'TARGET')
    const deployed = connectors !== null && Object.keys(connectors).length > 0
    const jdbc = jdbcSinkAggregate(connectors)
    return {
      srcDb: {
        label: source ? source.name : 'Oracle SRC',
        sub: source ? `${source.host}:${source.port}/${source.databaseName}` : '연결 미등록',
        status: source ? 'ok' : 'none',
      },
      source: {
        label: 'dz-source',
        sub: 'Debezium Oracle · LogMiner',
        status: connectorStatus(connectors, 'dz-source'),
      },
      kafka: {
        label: 'Kafka',
        sub: 'KRaft · 테이블당 토픽 1개',
        status: connectors === null ? 'crit' : 'ok',
      },
      jdbcSink: {
        label: 'dz-jdbc-sink',
        sub: jdbc.count > 0 ? `PK upsert · 테이블별 ${jdbc.count}개` : 'PK upsert · 실 적재',
        status: jdbc.status,
      },
      targetDb: {
        label: target ? target.name : 'Oracle TGT',
        sub: target ? `${target.host}:${target.port}/${target.databaseName}` : '연결 미등록',
        status: target ? 'ok' : 'none',
      },
      icebergSink: {
        label: 'dz-iceberg-sink',
        sub: 'append-only changelog',
        status: connectorStatus(connectors, 'dz-iceberg-sink'),
      },
      iceberg: {
        label: 'Iceberg / MinIO',
        sub: 'changelog · 복구 원본',
        status: deployed ? 'ok' : 'none',
      },
      recovery: {
        label: 'recovery-job',
        sub: 'scan → 재발행 (평시 정지)',
        status: 'none',
      },
    }
  }, [connectors, connections])

  const toPoints = <T,>(rows: T[], ts: (r: T) => string, v: (r: T) => number) =>
    rows.map((r) => ({ ts: Date.parse(ts(r)), value: v(r) }))

  const timeFormat = period.key === 'MIN'
    ? undefined // 기본 HH:MM
    : (ts: number) => {
        const d = new Date(ts)
        const md = `${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
        return period.key === 'DAY' ? md : `${md} ${String(d.getHours()).padStart(2, '0')}시`
      }

  const tableLabel = table === 'all'
    ? '전체 테이블'
    : table.replace(/^dz\./, '')
  const filteredTables = tables.filter((t) =>
    `${t.schemaName}.${t.tableName}`.toLowerCase().includes(tableQuery.toLowerCase()))

  // --- KPI 계산 (전부 이미 폴링 중인 API에서 파생 — 새 backend 엔드포인트 없음) ---
  const running = connectors ? Object.values(connectors).filter((c) => effectiveState(c) === 'RUNNING').length : 0
  const total = connectors ? Object.keys(connectors).length : 0

  // 버킷 값은 해상도 구간의 합 — ev/s로 환산해 표기. 마지막 버킷은 미완결이라 제외
  const bucketSec = { MIN: 60, HOUR: 3600, DAY: 86400 }[period.key]
  const completeBuckets = dashboard && dashboard.throughput.length > 1
    ? dashboard.throughput.slice(0, -1)
    : dashboard?.throughput ?? []
  const throughputLast = completeBuckets.length > 0
    ? completeBuckets[completeBuckets.length - 1].publish / bucketSec
    : null
  const throughputAvg = completeBuckets.length > 0
    ? completeBuckets.reduce((s, r) => s + r.publish, 0) / completeBuckets.length / bucketSec
    : null
  const throughputPeak = completeBuckets.length > 0
    ? Math.max(...completeBuckets.map((r) => r.publish)) / bucketSec
    : null

  const lagRows = (tableMetrics ?? [])
    .map((m) => ({ name: `${m.schemaName}.${m.tableName}`, lag: m.jdbcLag }))
    .sort((a, b) => b.lag - a.lag)
  const maxLag = lagRows[0] ?? null
  const overLagCount = lagRows.filter((r) => r.lag > LAG_WARN).length

  const pendingDdlList = (ddlEvents ?? []).filter((e) => e.state === 'DETECTED')
  const oldestDdlMs = pendingDdlList.length > 0
    ? Math.min(...pendingDdlList.map((e) => e.eventTsMs))
    : null

  // 주의 필요: lag 초과 테이블 + DDL 승인 대기
  const attention = [
    ...lagRows.filter((r) => r.lag > LAG_WARN).map((r) => ({
      kind: 'lag' as const,
      name: r.name,
      detail: `${r.lag.toLocaleString()}건 · 임계 ${LAG_WARN}건 초과`,
    })),
    ...pendingDdlList.map((e) => ({
      kind: 'ddl' as const,
      name: e.schemaName && e.tableName ? `${e.schemaName}.${e.tableName}` : '(테이블 미상)',
      detail: `${e.ddlText.split('\n')[0].slice(0, 40)} · 승인 대기`,
    })),
  ]

  return (
    <div className="h-full overflow-y-auto">
      {connectors === null && (
        <div className="border-b border-border bg-card px-4 py-2 text-xs text-warn">
          엔진에 연결할 수 없습니다 — 상태는 표시용 기본값입니다.
        </div>
      )}
      <div className="flex flex-col gap-4 p-5">
        {/* KPI 4 */}
        <div className="grid grid-cols-4 gap-4">
          <Card>
            <CardContent className="flex flex-col gap-1.5 py-3.5">
              <div className="text-xs text-ink-2">커넥터</div>
              <div className="text-2xl font-semibold leading-tight tracking-tight text-foreground">
                {connectors === null ? '—' : running}
                <small className="ml-1 text-[13px] font-medium text-ink-3">/ {total} running</small>
              </div>
              <div className="flex flex-wrap gap-1.5">
                <StatusPill variant={STATUS_TO_PILL[topo.source.status]}>source</StatusPill>
                <StatusPill variant={STATUS_TO_PILL[topo.jdbcSink.status]}>jdbc</StatusPill>
                <StatusPill variant={STATUS_TO_PILL[topo.icebergSink.status]}>iceberg</StatusPill>
                <StatusPill variant={STATUS_TO_PILL[topo.recovery.status]}>recovery</StatusPill>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="flex flex-col gap-1.5 py-3.5">
              <div className="text-xs text-ink-2">처리량</div>
              <div className="text-2xl font-semibold leading-tight tracking-tight text-foreground">
                {throughputLast === null ? '—' : Math.round(throughputLast).toLocaleString()}
                <small className="ml-1 text-[13px] font-medium text-ink-3">ev/s</small>
              </div>
              <div className="text-[11.5px] text-ink-3">
                {throughputAvg === null
                  ? '수집 대기 중'
                  : `${period.label} 평균 ${throughputAvg.toFixed(0)} · peak ${throughputPeak!.toFixed(0)}`}
              </div>
            </CardContent>
          </Card>

          <Card className={overLagCount > 0 ? 'shadow-[var(--shadow-card),inset_0_0_0_1px_var(--warn-soft)]' : undefined}>
            <CardContent className="flex flex-col gap-1.5 py-3.5">
              <div className="flex items-center gap-2 text-xs text-ink-2">
                최대 lag
                {maxLag && overLagCount > 0 && (
                  <StatusPill variant="warn" className="font-mono">{maxLag.name}</StatusPill>
                )}
              </div>
              <div className="text-2xl font-semibold leading-tight tracking-tight text-foreground">
                {maxLag === null ? '—' : maxLag.lag.toLocaleString()}
                <small className="ml-1 text-[13px] font-medium text-ink-3">건</small>
              </div>
              <div className="text-[11.5px] text-ink-3">
                {tableMetrics === null
                  ? '지표 조회 실패'
                  : `경고 임계 ${LAG_WARN}건 · ${lagRows.length}개 테이블 중 ${overLagCount}개 초과`}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="flex flex-col gap-1.5 py-3.5">
              <div className="text-xs text-ink-2">미승인 DDL</div>
              <div className="text-2xl font-semibold leading-tight tracking-tight text-foreground">
                {ddlEvents === null ? '—' : pendingDdlList.length}
                <small className="ml-1 text-[13px] font-medium text-ink-3">건 대기</small>
              </div>
              <div className="text-[11.5px] text-ink-3">
                {oldestDdlMs === null ? (
                  '대기 중인 DDL 없음'
                ) : (
                  <>
                    가장 오래된 것 {relativeTime(oldestDdlMs)} ·{' '}
                    <button
                      className="font-semibold text-primary hover:underline"
                      onClick={() => onNavigate?.('ddl')}
                    >
                      검토 →
                    </button>
                  </>
                )}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* 토폴로지 + 주의 필요 */}
        <div className="grid gap-4" style={{ gridTemplateColumns: 'minmax(0,1fr) 380px' }}>
          <Card className="flex flex-col">
            <CardHeader>
              <CardTitle>토폴로지</CardTitle>
              <span className="font-mono text-xs text-ink-3">
                {tables.length} tables
              </span>
              <GhostButton className="ml-auto" onClick={() => onNavigate?.('tables')}>
                테이블 전체
              </GhostButton>
            </CardHeader>
            <div className="flex flex-1 items-center justify-center p-2" style={{ minHeight: 260 }}>
              <TopologySvg data={topo} />
            </div>
          </Card>

          <Card className="flex flex-col">
            <CardHeader>
              <CardTitle>주의 필요</CardTitle>
              {attention.length > 0 && (
                <StatusPill variant="warn" className="ml-auto" dot={false}>
                  {attention.length}
                </StatusPill>
              )}
            </CardHeader>
            <div className="flex-1 overflow-auto">
              {attention.length === 0 ? (
                <p className="p-4 text-xs text-ink-3">주의가 필요한 항목이 없습니다.</p>
              ) : (
                attention.map((a, i) => (
                  <div
                    key={`${a.kind}-${a.name}-${i}`}
                    className="flex items-center gap-2.5 border-t border-border px-4 py-2.5 text-[12.5px] first:border-t-0"
                  >
                    <StatusPill variant="warn">{a.kind === 'lag' ? 'lag' : 'DDL'}</StatusPill>
                    <div className="min-w-0 flex-1">
                      <div className="truncate font-mono text-foreground">{a.name}</div>
                      <div className="truncate text-[11.5px] text-ink-3">{a.detail}</div>
                    </div>
                    <GhostButton onClick={() => onNavigate?.(a.kind === 'lag' ? 'tables' : 'ddl')}>
                      {a.kind === 'lag' ? '보기' : '검토'}
                    </GhostButton>
                  </div>
                ))
              )}
            </div>
          </Card>
        </div>

        {/* 필터 행: 테이블 콤보박스(검색) + 주기 */}
        <div className="flex items-center gap-2">
          <div ref={comboRef} className="relative">
            <button
              className="w-56 rounded-md border border-line-2 bg-card px-3 py-1.5 text-left font-mono text-xs text-foreground hover:border-primary"
              onClick={() => setTableOpen((o) => !o)}
            >
              {tableLabel} <span className="float-right text-ink-3">▾</span>
            </button>
            {tableOpen && (
              <div className="absolute z-10 mt-1 w-72 rounded-md border border-border bg-card shadow-[var(--shadow-card)]">
                <input
                  autoFocus
                  value={tableQuery}
                  onChange={(e) => setTableQuery(e.target.value)}
                  placeholder="테이블 검색…"
                  className="w-full border-b border-border bg-transparent px-3 py-1.5 text-xs outline-none"
                />
                <ul className="max-h-56 overflow-y-auto py-1 text-xs">
                  <li>
                    <button
                      className="w-full px-3 py-1.5 text-left hover:bg-surface-2"
                      onClick={() => { setTable('all'); setTableOpen(false); setTableQuery('') }}
                    >
                      전체 테이블 (합계)
                    </button>
                  </li>
                  {filteredTables.map((t) => {
                    const topic = `dz.${t.schemaName}.${t.tableName}`
                    return (
                      <li key={topic}>
                        <button
                          className="w-full px-3 py-1.5 text-left font-mono hover:bg-surface-2"
                          onClick={() => { setTable(topic); setTableOpen(false); setTableQuery('') }}
                        >
                          {t.schemaName}.{t.tableName}
                        </button>
                      </li>
                    )
                  })}
                  {filteredTables.length === 0 && (
                    <li className="px-3 py-1.5 text-ink-3">검색 결과 없음</li>
                  )}
                </ul>
              </div>
            )}
          </div>
          <Segmented options={PERIODS} value={period.key} onChange={(k) => setPeriod(PERIODS.find((p) => p.key === k)!)} />
        </div>

        {/* 처리량 차트 + 최근 이벤트 */}
        <div className="grid gap-4" style={{ gridTemplateColumns: 'minmax(0,1fr) 380px' }}>
          <Card>
            <CardHeader>
              <CardTitle>처리량 · lag</CardTitle>
              <span className="font-mono text-xs text-ink-3">{tableLabel} · {period.label}</span>
            </CardHeader>
            <CardContent className="py-3">
              <LineChart
                timeFormat={timeFormat}
                series={[
                  {
                    name: '발행 (캡처)',
                    color: CHART_SERIES_COLORS[0],
                    points: toPoints(dashboard?.throughput ?? [], (r) => r.ts, (r) => r.publish),
                  },
                  {
                    name: 'apply (타깃)',
                    color: CHART_SERIES_COLORS[1],
                    points: toPoints(dashboard?.throughput ?? [], (r) => r.ts, (r) => r.apply),
                  },
                ]}
              />
            </CardContent>
          </Card>

          <Card className="flex flex-col">
            <CardHeader>
              <CardTitle>최근 이벤트</CardTitle>
              <button
                className="ml-auto flex items-center gap-1 text-xs font-medium text-primary hover:underline"
                onClick={() => onNavigate?.('tables')}
              >
                전체 <ArrowRight className="size-3" />
              </button>
            </CardHeader>
            <div className="flex-1 overflow-auto">
              {events.length === 0 ? (
                <p className="p-4 text-xs text-ink-3">이벤트 없음</p>
              ) : (
                events.map((e, i) => (
                  <div key={i} className="grid grid-cols-[62px_16px_1fr] gap-x-2.5 border-t border-border px-4 py-2.5 text-[12.5px] first:border-t-0">
                    <span className="pt-px font-mono text-[11.5px] text-ink-3">
                      {e.occurredAt.slice(11, 19)}
                    </span>
                    <span
                      className="mt-[5px] size-2 rounded-full"
                      style={{
                        background:
                          e.severity === 'ERROR'
                            ? 'var(--crit)'
                            : e.severity === 'WARN'
                              ? 'var(--warn)'
                              : 'var(--ok)',
                      }}
                    />
                    <div>
                      <div className="text-foreground">{e.eventType}</div>
                      <div className="mt-0.5 truncate font-mono text-[11px] text-ink-3" title={e.message}>
                        {e.message}
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </Card>
        </div>

        {/* lag 차트 + 컴포넌트 자원 */}
        <div className="grid gap-4" style={{ gridTemplateColumns: 'minmax(0,1fr) 380px' }}>
          <Card>
            <CardHeader>
              <CardTitle>sink lag 추이</CardTitle>
              <span className="font-mono text-xs text-ink-3">
                {tableLabel} · {period.label}{period.key !== 'MIN' && ' · 구간 최대'}
              </span>
            </CardHeader>
            <CardContent className="py-3">
              <LineChart
                timeFormat={timeFormat}
                series={[
                  {
                    name: 'JDBC lag',
                    color: CHART_SERIES_COLORS[0],
                    points: toPoints(dashboard?.lag ?? [], (r) => r.ts, (r) => r.jdbc),
                  },
                  {
                    name: 'Iceberg lag',
                    color: CHART_SERIES_COLORS[1],
                    points: toPoints(dashboard?.lag ?? [], (r) => r.ts, (r) => r.iceberg),
                  },
                ]}
              />
            </CardContent>
          </Card>

          <Card className="flex flex-col">
            <CardHeader>
              <CardTitle>컴포넌트 자원</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 overflow-auto py-3">
              {(dashboard?.resources ?? []).length === 0 ? (
                <p className="text-xs text-ink-3">수집 대기 중…</p>
              ) : (
                <table className="w-full font-mono text-[11px]">
                  <tbody>
                    {dashboard!.resources
                      .slice()
                      .sort((a, b) => b.rssMb - a.rssMb)
                      .map((r) => (
                        <tr key={r.component}>
                          <td className="py-0.5 text-foreground">{r.component}</td>
                          <td className="py-0.5 text-right text-ink-3">CPU {r.cpuPct.toFixed(1)}%</td>
                          <td className="py-0.5 text-right text-ink-3">
                            {r.rssMb >= 1024 ? (r.rssMb / 1024).toFixed(1) + ' GB' : r.rssMb + ' MB'}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}

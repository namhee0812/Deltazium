/**
 * 파일명 : TopologyPanel.tsx
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : 대시보드 — 토폴로지(자체 SVG) + 처리량·lag 시계열(테이블 선택·주기 선택) +
 * 컴포넌트 자원(/proc 실측) + 최근 운영 이벤트.
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
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '@/lib/api'
import { effectiveState } from '@/lib/connect'
import type { ConnectorStates } from '@/lib/connect'
import type { DbConnection } from '@/features/connections/types'
import { CHART_SERIES_COLORS, LineChart } from '@/components/LineChart'
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

/** 주기 선택 — 해상도와 조회 폭을 묶는다 (Grafana식 기간별 자동 해상도) */
const PERIODS = [
  { key: 'MIN', label: '1분 (6시간)', hours: 6 },
  { key: 'HOUR', label: '1시간 (7일)', hours: 168 },
  { key: 'DAY', label: '1일 (90일)', hours: 2160 },
] as const

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

export function TopologyPanel() {
  const [connectors, setConnectors] = useState<ConnectorStates | null>(null)
  const [connections, setConnections] = useState<DbConnection[]>([])
  const [dashboard, setDashboard] = useState<Dashboard | null>(null)
  const [events, setEvents] = useState<Ev[]>([])
  const [tables, setTables] = useState<RegisteredTable[]>([])
  const [period, setPeriod] = useState<(typeof PERIODS)[number]>(PERIODS[0])
  const [table, setTable] = useState<string>('all') // 'all' 또는 topic
  const [tableQuery, setTableQuery] = useState('')
  const [tableOpen, setTableOpen] = useState(false)
  const comboRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const load = () => {
      api<ConnectorStates>('/api/connectors').then(setConnectors).catch(() => setConnectors(null))
      api<DbConnection[]>('/api/connections').then(setConnections).catch(() => setConnections([]))
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

  return (
    <div className="h-full overflow-y-auto">
      {connectors === null && (
        <div className="border-b border-border bg-card px-4 py-2 text-xs text-warn">
          backend(8090)에 연결할 수 없습니다 — 상태는 표시용 기본값입니다.
        </div>
      )}
      <div className="grid gap-3 p-3" style={{ gridTemplateColumns: 'minmax(0,1fr) 300px' }}>
        {/* 토폴로지 (자체 SVG) */}
        <div className="h-[340px] rounded-lg border border-border bg-card p-2">
          <TopologySvg data={topo} />
        </div>

        {/* 우측: 컴포넌트 자원 + 최근 이벤트 */}
        <div className="flex flex-col gap-3">
          <div className="rounded-lg border border-border bg-card p-3">
            <h3 className="mb-2 text-xs font-semibold text-muted-foreground">
              컴포넌트 자원 <span className="font-normal">(/proc 실측, 1분 주기)</span>
            </h3>
            {(dashboard?.resources ?? []).length === 0 ? (
              <p className="text-xs text-muted-foreground">수집 대기 중…</p>
            ) : (
              <table className="w-full font-mono text-[11px]">
                <tbody>
                  {dashboard!.resources
                    .slice()
                    .sort((a, b) => b.rssMb - a.rssMb)
                    .map((r) => (
                      <tr key={r.component}>
                        <td className="py-0.5 text-foreground">{r.component}</td>
                        <td className="py-0.5 text-right text-muted-foreground">
                          CPU {r.cpuPct.toFixed(1)}%
                        </td>
                        <td className="py-0.5 text-right text-muted-foreground">
                          {r.rssMb >= 1024
                            ? (r.rssMb / 1024).toFixed(1) + ' GB'
                            : r.rssMb + ' MB'}
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
            )}
          </div>
          <div className="min-h-0 flex-1 rounded-lg border border-border bg-card p-3">
            <h3 className="mb-2 text-xs font-semibold text-muted-foreground">최근 운영 이벤트</h3>
            {events.length === 0 ? (
              <p className="text-xs text-muted-foreground">이벤트 없음</p>
            ) : (
              <ul className="flex flex-col gap-1.5 text-[11px]">
                {events.map((e, i) => (
                  <li key={i} className="flex items-baseline gap-1.5">
                    <span
                      className={
                        e.severity === 'ERROR'
                          ? 'text-crit'
                          : e.severity === 'WARN'
                            ? 'text-warn'
                            : 'text-ok'
                      }
                    >
                      ●
                    </span>
                    <span className="font-mono text-muted-foreground">
                      {e.occurredAt.slice(5, 16).replace('T', ' ')}
                    </span>
                    <span className="truncate text-foreground" title={e.message}>
                      {e.eventType}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>

        {/* 필터 행: 테이블 콤보박스(검색) + 주기 */}
        <div className="col-span-2 flex items-center gap-2">
          <div ref={comboRef} className="relative">
            <button
              className="w-56 rounded-md border border-border bg-card px-3 py-1.5 text-left font-mono text-xs text-foreground hover:border-ring"
              onClick={() => setTableOpen((o) => !o)}
            >
              {tableLabel} <span className="float-right text-muted-foreground">▾</span>
            </button>
            {tableOpen && (
              <div className="absolute z-10 mt-1 w-72 rounded-md border border-border bg-card shadow-lg">
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
                      className="w-full px-3 py-1.5 text-left hover:bg-surface2"
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
                          className="w-full px-3 py-1.5 text-left font-mono hover:bg-surface2"
                          onClick={() => { setTable(topic); setTableOpen(false); setTableQuery('') }}
                        >
                          {t.schemaName}.{t.tableName}
                        </button>
                      </li>
                    )
                  })}
                  {filteredTables.length === 0 && (
                    <li className="px-3 py-1.5 text-muted-foreground">검색 결과 없음</li>
                  )}
                </ul>
              </div>
            )}
          </div>
          <div className="flex overflow-hidden rounded-md border border-border">
            {PERIODS.map((p) => (
              <button
                key={p.key}
                className={`px-3 py-1.5 text-xs ${
                  period.key === p.key
                    ? 'bg-surface2 font-semibold text-foreground'
                    : 'bg-card text-muted-foreground hover:text-foreground'
                }`}
                onClick={() => setPeriod(p)}
              >
                {p.label}
              </button>
            ))}
          </div>
          <span className="text-[11px] text-muted-foreground">
            원본 1분(48h 보존) → 시간(60일) → 일(1년) 자동 롤업
          </span>
        </div>

        {/* 시계열 차트 */}
        <div className="rounded-lg border border-border bg-card p-3">
          <h3 className="mb-1 text-xs font-semibold text-muted-foreground">
            이벤트 처리량 <span className="font-normal">({tableLabel} · {period.label})</span>
          </h3>
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
        </div>
        <div className="rounded-lg border border-border bg-card p-3">
          <h3 className="mb-1 text-xs font-semibold text-muted-foreground">
            sink lag 추이 <span className="font-normal">({tableLabel} · {period.label}
            {period.key !== 'MIN' && ' · 구간 최대'})</span>
          </h3>
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
        </div>
      </div>
    </div>
  )
}

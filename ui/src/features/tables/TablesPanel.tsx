import { useCallback, useEffect, useRef, useState } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { Spark } from '@/components/Spark'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { api } from '@/lib/api'

/* 테이블 모니터링 (ui-reference v2·v3) — 전부 실측:
   행 = 등록 테이블, 총 이벤트 = 토픽 end offset, 이벤트/s = offset 증가율,
   LAG = sink consumer lag (records). 5초 폴링, 스파크라인은 클라이언트 누적 */

interface TableMetrics {
  schemaName: string
  tableName: string
  topic: string
  totalEvents: number
  eventsPerSec: number
  jdbcLag: number
  icebergLag: number
}

interface RegisteredTable {
  id: number
  schemaName: string
  tableName: string
}

type ConnectorStates = Record<string, { status?: { connector?: { state?: string } } }>

const COLOR = { ok: '#56D89C', warn: '#F5B453', crit: '#F0647A', accent: '#53C8E8', dim: '#8A97B4' }

const columnHelper = createColumnHelper<TableMetrics>()

const suffix = (m: { schemaName: string; tableName: string }) =>
  `${m.schemaName}_${m.tableName}`.toLowerCase()

export function TablesPanel({ refreshKey = 0 }: { refreshKey?: number }) {
  const [rows, setRows] = useState<TableMetrics[] | null>(null)
  const [registered, setRegistered] = useState<RegisteredTable[]>([])
  const [connectors, setConnectors] = useState<ConnectorStates>({})
  const [error, setError] = useState<string | null>(null)
  const [globalFilter, setGlobalFilter] = useState('')
  const [deleting, setDeleting] = useState<TableMetrics | null>(null)
  const [dropChangelog, setDropChangelog] = useState(false)
  const [busy, setBusy] = useState(false)
  // 토픽별 이벤트/s 이력 (스파크라인용, 최근 24포인트)
  const history = useRef<Record<string, number[]>>({})
  const [, forceRender] = useState(0)

  const load = useCallback(() => {
    api<TableMetrics[]>('/api/metrics/tables')
      .then((data) => {
        for (const m of data) {
          const h = history.current[m.topic] ?? []
          history.current[m.topic] = [...h.slice(-23), m.eventsPerSec]
        }
        setRows(data)
        setError(null)
      })
      .catch((e: Error) => setError(e.message))
    api<RegisteredTable[]>('/api/registrations').then(setRegistered).catch(() => {})
    api<ConnectorStates>('/api/connectors').then(setConnectors).catch(() => {})
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [load, refreshKey])

  const idOf = (m: TableMetrics) =>
    registered.find((r) => r.schemaName === m.schemaName && r.tableName === m.tableName)?.id

  const sinkState = (m: TableMetrics) =>
    connectors[`dz-jdbc-sink-${suffix(m)}`]?.status?.connector?.state ?? 'N/A'

  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true)
    setError(null)
    try {
      await fn()
      load()
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setBusy(false)
    }
  }

  const pauseResume = (m: TableMetrics) => {
    const id = idOf(m)
    if (id == null) return
    const action = sinkState(m) === 'PAUSED' ? 'resume' : 'pause'
    void act(() => api(`/api/registrations/${id}/${action}`, { method: 'POST' }))
  }

  const confirmDelete = () => {
    const id = deleting ? idOf(deleting) : null
    if (id == null) return
    void act(async () => {
      await api(`/api/registrations/${id}?dropChangelog=${dropChangelog}`, { method: 'DELETE' })
      setDeleting(null)
      setDropChangelog(false)
    })
  }

  const columns = [
    columnHelper.display({
      id: 'status',
      header: '',
      cell: ({ row }) => {
        const state = sinkState(row.original)
        const color =
          state === 'RUNNING'
            ? row.original.jdbcLag > 100
              ? COLOR.warn
              : COLOR.ok
            : state === 'PAUSED'
              ? COLOR.warn
              : state === 'N/A'
                ? COLOR.dim
                : COLOR.crit
        return <span title={state} style={{ color }}>●</span>
      },
    }),
    columnHelper.accessor((m) => `${m.schemaName}.${m.tableName}`, {
      id: 'table',
      header: 'TABLE',
      cell: ({ row }) => (
        <span className="font-mono">
          {row.original.schemaName}.<b className="text-foreground">{row.original.tableName}</b>
        </span>
      ),
    }),
    columnHelper.display({
      id: 'topic',
      header: 'TOPIC',
      cell: ({ row }) => (
        <span className="font-mono text-[11px] text-muted-foreground">{row.original.topic}</span>
      ),
    }),
    columnHelper.accessor('totalEvents', {
      header: 'EVENTS',
      cell: ({ getValue }) => <span className="font-mono">{getValue().toLocaleString()}</span>,
    }),
    columnHelper.display({
      id: 'rate',
      header: 'EVENTS/s (실측)',
      cell: ({ row }) => {
        const h = history.current[row.original.topic] ?? []
        return (
          <div className="flex items-center gap-2">
            {h.length > 1 && <Spark data={h} color={COLOR.accent} />}
            <span className="font-mono text-[11px] text-muted-foreground">
              {row.original.eventsPerSec.toFixed(1)}/s
            </span>
          </div>
        )
      },
    }),
    columnHelper.accessor('jdbcLag', {
      header: 'JDBC LAG',
      cell: ({ getValue }) => (
        <span className={`font-mono ${getValue() > 100 ? 'text-warn' : 'text-foreground'}`}>
          {getValue().toLocaleString()}
        </span>
      ),
    }),
    columnHelper.accessor('icebergLag', {
      header: 'ICEBERG LAG',
      cell: ({ getValue }) => (
        <span className={`font-mono ${getValue() > 1000 ? 'text-warn' : 'text-foreground'}`}>
          {getValue().toLocaleString()}
        </span>
      ),
    }),
    columnHelper.display({
      id: 'actions',
      header: '',
      cell: ({ row }) => (
        <div className="flex justify-end gap-1">
          <Button
            variant="outline"
            size="sm"
            disabled={busy || sinkState(row.original) === 'N/A'}
            onClick={() => pauseResume(row.original)}
          >
            {sinkState(row.original) === 'PAUSED' ? '재개' : '정지'}
          </Button>
          <Button
            variant="destructive"
            size="sm"
            disabled={busy}
            onClick={() => {
              setDropChangelog(false)
              setDeleting(row.original)
            }}
          >
            삭제
          </Button>
        </div>
      ),
    }),
  ]

  const table = useReactTable({
    data: rows ?? [],
    columns,
    state: { globalFilter },
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    globalFilterFn: (row, _col, value: string) =>
      `${row.original.schemaName}.${row.original.tableName}`
        .toLowerCase()
        .includes(value.toLowerCase()),
  })

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex items-center gap-3 px-4 py-3">
        <Input
          value={globalFilter}
          onChange={(e) => setGlobalFilter(e.target.value)}
          placeholder="스키마.테이블 검색"
          className="w-56"
        />
        <button
          className="text-xs text-muted-foreground hover:text-foreground"
          onClick={() => {
            forceRender((n) => n + 1)
            load()
          }}
        >
          새로고침
        </button>
        <span className="ml-auto font-mono text-[11px] text-muted-foreground">
          {table.getRowModel().rows.length} tables · ICEBERG LAG은 커밋 주기(60s)만큼 지연이 정상
        </span>
      </div>

      {error && (
        <p className="px-4 pb-2 text-sm text-destructive">지표 조회 실패: {error}</p>
      )}
      {rows !== null && rows.length === 0 && !error && (
        <p className="px-4 pb-2 text-sm text-muted-foreground">
          등록된 테이블이 없습니다 — 상단 [＋ CDC 등록]으로 시작하세요.
        </p>
      )}

      <div className="flex-1 overflow-y-auto px-4 pb-4">
        <table className="w-full border-collapse text-[12.5px]">
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id} className="text-left font-mono text-[10.5px] text-muted-foreground">
                {hg.headers.map((h) => (
                  <th key={h.id} className="border-b border-border px-2.5 py-2 font-medium">
                    {flexRender(h.column.columnDef.header, h.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <tr key={row.id} className="hover:bg-secondary/60">
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className="px-2.5 py-2.5">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Dialog open={deleting !== null} onOpenChange={(o) => !o && setDeleting(null)}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>
              CDC 등록 삭제 — {deleting?.schemaName}.{deleting?.tableName}
            </DialogTitle>
            <DialogDescription>
              커넥터에서 제거되고 등록 정보(매핑 포함)가 삭제됩니다. 타깃 테이블의 데이터는
              건드리지 않습니다.
            </DialogDescription>
          </DialogHeader>
          <label className="flex items-start gap-2 text-sm">
            <input
              type="checkbox"
              className="mt-0.5"
              checked={dropChangelog}
              onChange={(e) => setDropChangelog(e.target.checked)}
            />
            <span>
              changelog(S3/Iceberg) 데이터까지 삭제
              <span className="block text-xs text-crit">
                복구 원본이 사라집니다 — 이 테이블은 더 이상 SCN 재발행 복구를 할 수 없습니다.
                해제하면 changelog는 보존됩니다(기본).
              </span>
            </span>
          </label>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setDeleting(null)}>
              취소
            </Button>
            <Button variant="destructive" disabled={busy} onClick={confirmDelete}>
              {dropChangelog ? '삭제 (changelog 포함)' : '삭제 (changelog 보존)'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

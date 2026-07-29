import { useCallback, useEffect, useRef, useState } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { Spark } from '@/components/Spark'
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

const COLOR = { ok: '#56D89C', warn: '#F5B453', crit: '#F0647A', accent: '#53C8E8' }

const statusOf = (m: TableMetrics) =>
  m.jdbcLag > 100 || m.icebergLag > 1000 ? 'warn' : 'ok'

const columnHelper = createColumnHelper<TableMetrics>()

export function TablesPanel({ refreshKey = 0 }: { refreshKey?: number }) {
  const [rows, setRows] = useState<TableMetrics[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [globalFilter, setGlobalFilter] = useState('')
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
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [load, refreshKey])

  const columns = [
    columnHelper.display({
      id: 'status',
      header: '',
      cell: ({ row }) => (
        <span style={{ color: COLOR[statusOf(row.original)] }}>●</span>
      ),
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
    </div>
  )
}

import { useEffect, useMemo, useState } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { Spark } from '@/components/Spark'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { api } from '@/lib/api'
import { genMetrics, mockTables, tickMetrics } from '@/lib/mock'
import type { CdcTable, TableMetrics } from '@/lib/mock'

/* 테이블 모니터링 그리드 (ui-reference v2·v3).
   등록 테이블은 실데이터(/api/registrations), 메트릭(lag·DML 등)은 수집 기능 전까지 mock */

interface RegisteredTable {
  id: number
  schemaName: string
  tableName: string
}

const COLOR = { ok: '#56D89C', warn: '#F5B453', crit: '#F0647A', accent: '#53C8E8' }
const sColor = (s: CdcTable['status']) => COLOR[s]

const suppLogBadge: Record<CdcTable['suppLog'], string> = {
  full: 'bg-ok/10 text-ok',
  'pk-only': 'bg-warn/10 text-warn',
  none: 'bg-crit/10 text-crit',
}

const columnHelper = createColumnHelper<CdcTable>()

export function TablesPanel({ refreshKey = 0 }: { refreshKey?: number }) {
  const [registered, setRegistered] = useState<CdcTable[] | null>(null)
  const [metrics, setMetrics] = useState<Record<number, TableMetrics>>({})
  const [selectedId, setSelectedId] = useState<number>(4)
  const [globalFilter, setGlobalFilter] = useState('')

  useEffect(() => {
    api<RegisteredTable[]>('/api/registrations')
      .then((rows) =>
        setRegistered(
          rows.map((r) => ({
            id: r.id,
            schema: r.schemaName,
            name: r.tableName,
            targets: ['Oracle TGT'],
            suppLog: 'full' as const,
            status: 'ok' as const,
          })),
        ),
      )
      .catch(() => setRegistered(null))
  }, [refreshKey])

  // 등록 테이블이 있으면 실데이터, 없으면 mock 시드
  const rows = registered && registered.length > 0 ? registered : mockTables
  const isMockRows = !(registered && registered.length > 0)

  useEffect(() => {
    setMetrics(Object.fromEntries(rows.map((t) => [t.id, genMetrics(t)])))
    const id = setInterval(() => {
      setMetrics((m) => {
        const next: Record<number, TableMetrics> = {}
        for (const t of rows) next[t.id] = m[t.id] ? tickMetrics(t, m[t.id]) : genMetrics(t)
        return next
      })
    }, 1800)
    return () => clearInterval(id)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [registered])

  const columns = useMemo(
    () => [
      columnHelper.display({
        id: 'status',
        header: '',
        cell: ({ row }) => (
          <span style={{ color: sColor(row.original.status) }}>●</span>
        ),
      }),
      columnHelper.accessor((t) => `${t.schema}.${t.name}`, {
        id: 'table',
        header: 'TABLE',
        cell: ({ row }) => (
          <span className="font-mono">
            {row.original.schema}.<b className="text-foreground">{row.original.name}</b>
          </span>
        ),
      }),
      columnHelper.display({
        id: 'targets',
        header: 'TARGETS',
        cell: ({ row }) => (
          <span className="font-mono text-[11px] text-muted-foreground">
            {row.original.targets.join(' · ')}
          </span>
        ),
      }),
      columnHelper.display({
        id: 'dml',
        header: 'DML/s (5m)',
        cell: ({ row }) => {
          const m = metrics[row.original.id]
          return m ? (
            <Spark
              data={m.dml}
              color={row.original.status === 'ok' ? COLOR.accent : sColor(row.original.status)}
            />
          ) : null
        },
      }),
      columnHelper.display({
        id: 'iud',
        header: 'I / U / D',
        cell: ({ row }) => {
          const m = metrics[row.original.id]
          return m ? (
            <span className="font-mono text-[11px] text-muted-foreground">
              <span className="text-ok">{m.ins}</span> /{' '}
              <span className="text-warn">{m.upd}</span> /{' '}
              <span className="text-crit">{m.del}</span>
            </span>
          ) : null
        },
      }),
      columnHelper.display({
        id: 'lag',
        header: 'LAG',
        cell: ({ row }) => {
          const m = metrics[row.original.id]
          if (!m) return null
          const lag = Math.round(m.lag[m.lag.length - 1])
          return (
            <span className={`font-mono ${lag > 300 ? 'text-warn' : 'text-foreground'}`}>
              {lag}ms
            </span>
          )
        },
      }),
      columnHelper.display({
        id: 'scn',
        header: 'LAST SCN',
        cell: ({ row }) => (
          <span className="font-mono text-[11px] text-muted-foreground">
            {metrics[row.original.id]?.scn}
          </span>
        ),
      }),
      columnHelper.accessor('suppLog', {
        header: 'SUPP.LOG',
        cell: ({ getValue }) => (
          <span
            className={`rounded px-1.5 py-0.5 font-mono text-[10px] ${suppLogBadge[getValue()]}`}
          >
            {getValue()}
          </span>
        ),
      }),
    ],
    [metrics],
  )

  const table = useReactTable({
    data: rows,
    columns,
    state: { globalFilter },
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    globalFilterFn: (row, _col, value: string) =>
      `${row.original.schema}.${row.original.name}`.toLowerCase().includes(value.toLowerCase()),
  })

  const sel = rows.find((t) => t.id === selectedId)
  const selM = metrics[selectedId]

  return (
    <div className="flex h-full min-h-0">
      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex items-center gap-3 px-4 py-3">
          <Input
            value={globalFilter}
            onChange={(e) => setGlobalFilter(e.target.value)}
            placeholder="스키마.테이블 검색"
            className="w-56"
          />
          <Badge variant="outline" className="text-warn">
            {isMockRows ? 'mock 데이터' : '메트릭 mock (수집 기능 전)'}
          </Badge>
          <span className="ml-auto font-mono text-[11px] text-muted-foreground">
            {table.getRowModel().rows.length} tables
          </span>
        </div>
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
                <tr
                  key={row.id}
                  onClick={() => setSelectedId(row.original.id)}
                  className={`cursor-pointer hover:bg-secondary/60 ${
                    selectedId === row.original.id ? 'bg-secondary' : ''
                  }`}
                >
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

      {sel && selM && (
        <div className="w-[300px] overflow-y-auto border-l border-border bg-card p-4">
          <div className="font-mono text-[13px] font-semibold">
            {sel.schema}.{sel.name}
          </div>
          <div className="mt-1 font-mono text-[11px] text-muted-foreground">
            → {sel.targets.join(' · ')}
          </div>
          {(
            [
              { label: 'DML throughput', data: selM.dml, unit: 'ops/s', color: COLOR.accent },
              {
                label: 'replication lag',
                data: selM.lag,
                unit: 'ms',
                color: sel.status === 'ok' ? COLOR.accent : sColor(sel.status),
              },
            ] as const
          ).map((c) => (
            <div key={c.label} className="mt-3 rounded-[10px] border border-border bg-secondary p-3">
              <div className="font-mono text-[10.5px] text-muted-foreground">{c.label}</div>
              <div className="my-1 text-xl font-semibold">
                {Math.round(c.data[c.data.length - 1]).toLocaleString()}{' '}
                <span className="text-[11px] text-muted-foreground">{c.unit}</span>
              </div>
              <Spark data={[...c.data]} color={c.color} w={240} h={40} />
            </div>
          ))}
          <div className="mt-3 rounded-[10px] border border-border bg-secondary p-3 font-mono text-[11px] leading-loose text-muted-foreground">
            <div className="flex justify-between">
              <span>last SCN</span>
              <span className="text-foreground">{selM.scn}</span>
            </div>
            <div className="flex justify-between">
              <span>DDL events (24h)</span>
              <span className="text-foreground">1</span>
            </div>
          </div>
          {sel.suppLog !== 'full' && (
            <div
              className={`mt-3 rounded-[10px] border p-3 text-xs leading-relaxed ${
                sel.suppLog === 'none' ? 'border-crit bg-crit/10' : 'border-warn bg-warn/10'
              }`}
            >
              <b className={sel.suppLog === 'none' ? 'text-crit' : 'text-warn'}>
                supplemental logging {sel.suppLog}.
              </b>{' '}
              UPDATE 시 미변경 컬럼 유실 가능 — ALTER TABLE … ADD SUPPLEMENTAL LOG DATA (ALL)
              COLUMNS 권장.
            </div>
          )}
        </div>
      )}
    </div>
  )
}

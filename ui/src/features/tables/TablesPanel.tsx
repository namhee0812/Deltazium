/**
 * 파일명 : TablesPanel.tsx
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : 테이블 모니터링 그리드 — 실측 이벤트·lag, 정지/재개/삭제(changelog 삭제 선택 포함).
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | sink 상태 판정을 effectiveState(connector+task)로 교체
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 캡처(dz-source) 장애·일시정지 경고 배너 추가
 * |                          | - 감지 시각(이벤트)·원인 한 줄(Caused by)·전체 trace 펼침
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | FAILED 행 [재시도](task restart) + 장애 배지 원인 툴팁
 * |                          | 재스냅샷 워크플로 — 상시 액션(INITIAL) + 배너 [복구 시작]
 * |                          | (INITIAL/NO_DATA), notification 기반 진행 배너·go-live 표시
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 재스냅샷에 "타깃 비우고 재적재(완전 재구축)" 체크박스 —
 * |                          | 갭 DELETE 고아 행까지 정리 (기본 해제, INITIAL 전용)
 * --------------------------------------------------
 */
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
import { causeLine, effectiveState, traceOf } from '@/lib/connect'
import type { ConnectorStates } from '@/lib/connect'

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
      .catch((e: Error) => setError('지표 조회 실패: ' + e.message))
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

  // 캡처(dz-source) 전역 상태 — 행별 배지는 각 테이블의 jdbc-sink만 보므로,
  // 캡처가 죽으면 여기 배너로 알린다 (sink 초록 + lag 0 = 정상처럼 보이는 착시 방지)
  const sourceInfo = connectors['dz-source']
  const sourceState = sourceInfo ? effectiveState(sourceInfo) : null
  const sourceBroken = sourceState === 'FAILED'
  const [detectedAt, setDetectedAt] = useState<string | null>(null)

  useEffect(() => {
    if (!sourceBroken) {
      setDetectedAt(null)
      return
    }
    interface Ev { occurredAt: string; eventType: string; message: string }
    api<Ev[]>('/api/events?limit=100')
      .then((evs) => {
        const hit = evs.find(
          (e) => e.eventType === 'CONNECTOR_FAILED' && e.message.includes('dz-source'))
        setDetectedAt(hit ? hit.occurredAt.replace('T', ' ').slice(0, 16) : null)
      })
      .catch(() => setDetectedAt(null))
  }, [sourceBroken])

  const sinkState = (m: TableMetrics) => {
    const info = connectors[`dz-jdbc-sink-${suffix(m)}`]
    return info ? effectiveState(info) : 'N/A'
  }

  // 재스냅샷 — 상시 운영 액션(Qlik Reload·DMS Reload table에 해당) + 장애 복구 진입점 겸용
  interface SnapshotStatus {
    phase: 'NONE' | 'REQUESTED' | 'IN_PROGRESS' | 'COMPLETED' | 'ABORTED'
    currentTable: string | null
    tables: Record<string, number>
    startedAtMs: number | null
    completedAtMs: number | null
  }
  const [snapshot, setSnapshot] = useState<SnapshotStatus | null>(null)
  // 'routine'(평시 — INITIAL만) | 'recover'(장애 배너 — NO_DATA 옵션 포함)
  const [resnapDialog, setResnapDialog] = useState<'routine' | 'recover' | null>(null)
  const [resnapMode, setResnapMode] = useState<'INITIAL' | 'NO_DATA'>('INITIAL')
  const [truncateTarget, setTruncateTarget] = useState(false)

  useEffect(() => {
    const poll = () =>
      api<SnapshotStatus>('/api/capture/snapshot').then(setSnapshot).catch(() => setSnapshot(null))
    poll()
    const id = setInterval(poll, 5000)
    return () => clearInterval(id)
  }, [])

  const snapshotActive = snapshot?.phase === 'REQUESTED' || snapshot?.phase === 'IN_PROGRESS'
  const goLiveJustNow =
    snapshot?.phase === 'COMPLETED' &&
    snapshot.completedAtMs !== null &&
    Date.now() - snapshot.completedAtMs < 120_000

  const triggerResnapshot = () => {
    const mode = resnapDialog === 'recover' ? resnapMode : 'INITIAL'
    const truncate = mode === 'INITIAL' && truncateTarget
    setResnapDialog(null)
    setResnapMode('INITIAL')
    setTruncateTarget(false)
    void act(() =>
      api('/api/capture/resnapshot', {
        method: 'POST',
        body: JSON.stringify({ mode, truncateTarget: truncate }),
      }))
  }

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
        // 노랑은 "봐야 할 신호(lag 경고)" 전용 — 의도된 정지는 회색으로 구분
        const color =
          state === 'RUNNING'
            ? row.original.jdbcLag > 100
              ? COLOR.warn
              : COLOR.ok
            : state === 'PAUSED' || state === 'N/A'
              ? COLOR.dim
              : COLOR.crit
        return <span title={state} style={{ color }}>●</span>
      },
    }),
    columnHelper.accessor((m) => `${m.schemaName}.${m.tableName}`, {
      id: 'table',
      header: 'TABLE',
      cell: ({ row }) => {
        const state = sinkState(row.original)
        return (
          <span className="font-mono">
            {row.original.schemaName}.<b className="text-foreground">{row.original.tableName}</b>
            {state === 'PAUSED' && (
              <span className="ml-2 rounded bg-secondary px-1.5 py-0.5 font-mono text-[10px] text-muted-foreground">
                정지됨
              </span>
            )}
            {state === 'FAILED' && (
              <span
                className="ml-2 cursor-help rounded bg-crit/15 px-1.5 py-0.5 font-mono text-[10px] text-crit"
                title={causeLine(connectors[`dz-jdbc-sink-${suffix(row.original)}`]) ?? '원인: 이벤트 탭 참조'}
              >
                장애
              </span>
            )}
          </span>
        )
      },
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
      cell: ({ row, getValue }) => {
        // 정지 중 lag 증가는 당연한 결과 — 경고색 대신 회색 유지
        const paused = sinkState(row.original) === 'PAUSED'
        return (
          <span
            className={`font-mono ${
              paused
                ? 'text-muted-foreground'
                : getValue() > 100
                  ? 'text-warn'
                  : 'text-foreground'
            }`}
          >
            {getValue().toLocaleString()}
          </span>
        )
      },
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
          {sinkState(row.original) === 'FAILED' && (
            <Button
              variant="outline"
              size="sm"
              disabled={busy}
              title="원인 해결 후 실패 지점(offset)부터 재시도"
              onClick={() =>
                act(() =>
                  api(`/api/connectors/dz-jdbc-sink-${suffix(row.original)}/restart`, {
                    method: 'POST',
                  }))
              }
            >
              재시도
            </Button>
          )}
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
      {snapshotActive && (
        <div className="border-b border-[#53C8E8]/40 bg-[#53C8E8]/10 px-4 py-2.5 text-[13px]">
          <span className="font-semibold text-[#53C8E8]">
            {snapshot!.phase === 'REQUESTED' ? '⟳ 재스냅샷 요청됨' : '⟳ 초기 스냅샷 진행 중'}
          </span>
          <span className="ml-2 font-mono text-[12px] text-muted-foreground">
            {snapshot!.phase === 'REQUESTED' &&
              '커넥터 재기동 중 — 스냅샷 시작(STARTED) 알림 대기. 수 분째 그대로면 이벤트 탭·backend 로그 확인'}
            {snapshot!.phase === 'IN_PROGRESS' && (
              <>
                {Object.keys(snapshot!.tables).length}개 테이블 완료
                {' '}(총 {Object.values(snapshot!.tables).reduce((a, b) => a + b, 0).toLocaleString()}행)
                {snapshot!.currentTable && <> · 진행 중: {snapshot!.currentTable}</>}
                {' '}— 완료 시 자동으로 스트리밍(go-live) 전환
              </>
            )}
          </span>
        </div>
      )}
      {goLiveJustNow && !snapshotActive && (
        <div className="border-b border-ok/40 bg-ok/10 px-4 py-2 text-[13px] text-ok">
          ✓ go-live — 초기 스냅샷 완료, 스트리밍 재개됨
          <span className="ml-2 font-mono text-[11px] text-muted-foreground">
            {Object.entries(snapshot!.tables)
              .map(([t, r]) => `${t.split('.').slice(-1)[0]} ${r.toLocaleString()}행`)
              .join(' · ')}
          </span>
        </div>
      )}
      {sourceBroken && !snapshotActive && (
        <div className="border-b border-crit/40 bg-crit/10 px-4 py-2.5 text-[13px]">
          <div className="flex items-start gap-2">
            <span className="mt-0.5 text-crit">⛔</span>
            <div className="min-w-0 flex-1">
              <span className="font-semibold text-crit">
                캡처 정지 — 전 테이블 신규 변경 수집 중단
              </span>
              <span className="text-muted-foreground">
                {' '}(타깃 apply·changelog에도 새 이벤트가 흐르지 않습니다)
              </span>
              <Button
                variant="outline"
                size="sm"
                className="ml-3 h-6 border-crit/50 px-2 text-[11px] text-crit hover:bg-crit/15"
                disabled={busy}
                onClick={() => setResnapDialog('recover')}
              >
                복구 시작
              </Button>
              <div className="mt-1 font-mono text-[11px] text-muted-foreground">
                {detectedAt && <>감지: {detectedAt} · </>}
                원인: {causeLine(sourceInfo) ?? 'trace 없음 — 이벤트 탭 참조'}
              </div>
              {traceOf(sourceInfo) && (
                <details className="mt-1">
                  <summary className="cursor-pointer text-[11px] text-muted-foreground hover:text-foreground">
                    상세 보기 (전체 trace)
                  </summary>
                  <pre className="mt-1 max-h-48 overflow-auto rounded bg-surface2 p-2 text-[10px] leading-snug">
                    {traceOf(sourceInfo)}
                  </pre>
                </details>
              )}
            </div>
          </div>
        </div>
      )}
      {sourceState === 'PAUSED' && (
        <div className="border-b border-border bg-surface2 px-4 py-2 text-[13px] text-muted-foreground">
          ⏸ 캡처 일시정지 — 전 테이블 신규 변경 수집이 멈춰 있습니다 (재개 전까지 redo 보존 기간에 유의)
        </div>
      )}
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
        <button
          className="text-xs text-muted-foreground hover:text-foreground"
          title="전 테이블 초기 스냅샷부터 재기동 (타깃 표류·정합 불일치 복구용, PK upsert라 비파괴)"
          disabled={busy || snapshotActive}
          onClick={() => setResnapDialog('routine')}
        >
          재스냅샷
        </button>
        <span className="ml-auto font-mono text-[11px] text-muted-foreground">
          {table.getRowModel().rows.length} tables · ICEBERG LAG은 커밋 주기(60s)만큼 지연이 정상
        </span>
      </div>

      {error && <p className="px-4 pb-2 text-sm text-destructive">{error}</p>}
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

      <Dialog open={resnapDialog !== null} onOpenChange={(o) => !o && setResnapDialog(null)}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>
              {resnapDialog === 'recover' ? '캡처 복구' : '재스냅샷'}
            </DialogTitle>
            <DialogDescription>
              캡처 커넥터를 offset 리셋 후 재기동합니다. 전 테이블 대상이며(캡처는 공용),
              apply가 PK upsert(MERGE)라 타깃 데이터는 훼손되지 않습니다.
              소스 DB에 스냅샷 읽기 부하가 발생하고, changelog에 스냅샷 행이 추가 기록됩니다.
            </DialogDescription>
          </DialogHeader>
          {resnapDialog === 'recover' ? (
            <div className="flex flex-col gap-2 text-sm">
              <label className="flex items-start gap-2">
                <input
                  type="radio"
                  className="mt-1"
                  checked={resnapMode === 'INITIAL'}
                  onChange={() => setResnapMode('INITIAL')}
                />
                <span>
                  <b>초기 스냅샷부터</b> (권장)
                  <span className="block text-xs text-muted-foreground">
                    전 테이블 재적재 후 스트리밍 — 유실 없이 현재 상태로 재구축합니다.
                  </span>
                </span>
              </label>
              <label className="flex items-start gap-2">
                <input
                  type="radio"
                  className="mt-1"
                  checked={resnapMode === 'NO_DATA'}
                  onChange={() => setResnapMode('NO_DATA')}
                />
                <span>
                  <b>현재 시점부터</b> (갭 유실 수용)
                  <span className="block text-xs text-crit">
                    장애 시점부터 지금까지의 변경은 영구 유실됩니다. 버튼을 누르는 시점
                    전후로 진행 중이던 트랜잭션도 온전히 반영되지 않을 수 있습니다.
                  </span>
                </span>
              </label>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              초기 스냅샷부터 재기동합니다 — 타깃 표류(직접 DML)·정합 검증 불일치를
              소스 기준으로 복구할 때 사용하세요.
            </p>
          )}
          {(resnapDialog === 'routine' || resnapMode === 'INITIAL') && (
            <label className="flex items-start gap-2 text-sm">
              <input
                type="checkbox"
                className="mt-0.5"
                checked={truncateTarget}
                onChange={(e) => setTruncateTarget(e.target.checked)}
              />
              <span>
                타깃을 비우고 재적재 (완전 재구축)
                <span className="block text-xs text-crit">
                  타깃 테이블을 TRUNCATE한 뒤 채웁니다 — 소스에서 삭제된 행(고아)까지
                  정리되는 유일한 방법. 단, 스냅샷 완료까지 타깃 조회가 비어 보입니다.
                  순서는 자동 제어됩니다 (유입 차단 → sink 잔량 소진 → TRUNCATE → 스냅샷).
                </span>
              </span>
            </label>
          )}
          <DialogFooter>
            <Button variant="ghost" onClick={() => setResnapDialog(null)}>
              취소
            </Button>
            <Button
              variant={truncateTarget && resnapMode === 'INITIAL' ? 'destructive' : 'default'}
              disabled={busy}
              onClick={triggerResnapshot}
            >
              {resnapDialog === 'recover' && resnapMode === 'NO_DATA'
                ? '현재 시점부터 재개'
                : truncateTarget
                  ? '타깃 비우고 초기 스냅샷 시작'
                  : '초기 스냅샷 시작'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

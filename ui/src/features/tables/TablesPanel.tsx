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
 * 26. 08. 05.       | 최남희  | 재스냅샷을 단계 팝업(ResnapshotDialog)으로 개편 —
 * |                          | 배너는 run 상태 요약 + 클릭 시 팝업, 다이얼로그 로직 이관
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 내부 용어 정리 — lag 설명은 컬럼 툴팁으로, offset 용어 완화
 * --------------------------------------------------
 * 26. 08. 24.       | 최남희  | 행 원천을 /api/registrations로 분리 — /api/metrics/tables 실패해도
 * |                          | 테이블 목록은 유지되고 지표 셀만 "—"로 표시 (Kafka 다운 오검출 방지)
 * --------------------------------------------------
 * 26. 08. 27.       | 최남희  | 하드코딩 hex를 CSS 변수/토큰 클래스로 교체 — 라이트 테마 대응
 * 26. 08. 27.       | 최남희  | 토큰명 accent-cyan → brand (VS Code풍 중립 팔레트 전환으로 시안 아님)
 * --------------------------------------------------
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
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
import { ResnapshotDialog, RUN_ACTIVE } from './ResnapshotDialog'
import type { RunStatus } from './ResnapshotDialog'

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

/** 그리드 행 — 목록은 항상 /api/registrations(PG 조회, 항상 성공)에서 오고,
 * 지표(topic·이벤트·lag)는 /api/metrics/tables(Kafka AdminClient 조회) 성공 시에만 채워진다.
 * metrics가 null이면 지표 조회 실패/미완료 — 셀은 "—"로 표시한다. */
interface Row {
  id: number
  schemaName: string
  tableName: string
  metrics: TableMetrics | null
}

const COLOR = {
  ok: 'var(--ok)',
  warn: 'var(--warn)',
  crit: 'var(--crit)',
  accent: 'var(--brand)',
  dim: 'var(--chart-dim)',
}

const columnHelper = createColumnHelper<Row>()

const suffix = (m: { schemaName: string; tableName: string }) =>
  `${m.schemaName}_${m.tableName}`.toLowerCase()

const METRICS_ERROR_PREFIX = '지표 조회 실패(Kafka 연결 확인): '

export function TablesPanel({ refreshKey = 0 }: { refreshKey?: number }) {
  const [registered, setRegistered] = useState<RegisteredTable[] | null>(null)
  const [metrics, setMetrics] = useState<TableMetrics[] | null>(null)
  const [connectors, setConnectors] = useState<ConnectorStates>({})
  const [error, setError] = useState<string | null>(null)
  const [globalFilter, setGlobalFilter] = useState('')
  const [deleting, setDeleting] = useState<Row | null>(null)
  const [dropChangelog, setDropChangelog] = useState(false)
  const [busy, setBusy] = useState(false)
  // 토픽별 이벤트/s 이력 (스파크라인용, 최근 24포인트)
  const history = useRef<Record<string, number[]>>({})
  const [, forceRender] = useState(0)

  const load = useCallback(() => {
    api<RegisteredTable[]>('/api/registrations')
      .then(setRegistered)
      .catch((e: Error) => setError('테이블 목록 조회 실패: ' + e.message))
    api<TableMetrics[]>('/api/metrics/tables')
      .then((data) => {
        for (const m of data) {
          const h = history.current[m.topic] ?? []
          history.current[m.topic] = [...h.slice(-23), m.eventsPerSec]
        }
        setMetrics(data)
        setError(null)
      })
      .catch((e: Error) => {
        // 행 목록(registered)은 그대로 유지 — 지표만 "—"로 빠진다.
        setMetrics(null)
        setError(METRICS_ERROR_PREFIX + e.message)
      })
    api<ConnectorStates>('/api/connectors').then(setConnectors).catch(() => {})
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [load, refreshKey])

  const rows = useMemo<Row[] | null>(() => {
    if (registered === null) return null
    const byKey = new Map((metrics ?? []).map((m) => [`${m.schemaName}.${m.tableName}`, m]))
    return registered.map((r) => ({
      id: r.id,
      schemaName: r.schemaName,
      tableName: r.tableName,
      metrics: byKey.get(`${r.schemaName}.${r.tableName}`) ?? null,
    }))
  }, [registered, metrics])

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

  const sinkState = (m: Row) => {
    const info = connectors[`dz-jdbc-sink-${suffix(m)}`]
    return info ? effectiveState(info) : 'N/A'
  }

  // 재스냅샷 — 상시 운영 액션(Qlik Reload·DMS Reload table에 해당) + 장애 복구 진입점 겸용.
  // 진행은 backend 상태 기계(run)가 담당, 여기서는 배너 표시용으로만 폴링 (상세는 팝업)
  const [run, setRun] = useState<RunStatus | null>(null)
  // 'routine'(평시 — INITIAL만) | 'recover'(장애 배너 — NO_DATA 옵션 포함)
  const [resnapDialog, setResnapDialog] = useState<'routine' | 'recover' | null>(null)

  useEffect(() => {
    const poll = () =>
      api<RunStatus | undefined>('/api/capture/resnapshot/run')
        .then((r) => setRun(r ?? null))
        .catch(() => setRun(null))
    poll()
    const id = setInterval(poll, 5000)
    return () => clearInterval(id)
  }, [])

  const snapshotActive = RUN_ACTIVE(run)
  const [goLiveDismissed, setGoLiveDismissed] = useState<number | null>(null)
  const goLiveJustNow =
    run?.phase === 'DONE' &&
    run.finishedAtMs !== null &&
    Date.now() - run.finishedAtMs < 120_000 &&
    run.startedAtMs !== goLiveDismissed

  const RUN_PHASE_LABEL: Record<string, string> = {
    STOPPING_SOURCE: '① 유입 차단',
    DRAINING: '② 파이프 잔량 소진',
    AWAITING_DECISION: '③ 타깃 비우기 — 실행 주체 선택 대기',
    HELD: '③ 타깃 비우기 — 홀드 (DBA 실행 대기)',
    TRUNCATING: '③ 타깃 비우기 실행 중',
    RESETTING: '④ 캡처 초기화·재기동',
    SNAPSHOTTING: '⑤ 초기 스냅샷',
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

  const pauseResume = (m: Row) => {
    const action = sinkState(m) === 'PAUSED' ? 'resume' : 'pause'
    void act(() => api(`/api/registrations/${m.id}/${action}`, { method: 'POST' }))
  }

  const confirmDelete = () => {
    if (!deleting) return
    void act(async () => {
      await api(`/api/registrations/${deleting.id}?dropChangelog=${dropChangelog}`, { method: 'DELETE' })
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
        const lag = row.original.metrics?.jdbcLag ?? 0
        // 노랑은 "봐야 할 신호(lag 경고)" 전용 — 의도된 정지는 회색으로 구분
        const color =
          state === 'RUNNING'
            ? lag > 100
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
        <span className="font-mono text-[11px] text-muted-foreground">
          {row.original.metrics?.topic ?? '—'}
        </span>
      ),
    }),
    columnHelper.display({
      id: 'events',
      header: 'EVENTS',
      cell: ({ row }) => (
        <span className="font-mono">
          {row.original.metrics ? row.original.metrics.totalEvents.toLocaleString() : '—'}
        </span>
      ),
    }),
    columnHelper.display({
      id: 'rate',
      header: 'EVENTS/s (실측)',
      cell: ({ row }) => {
        const m = row.original.metrics
        if (!m) return <span className="font-mono text-[11px] text-muted-foreground">—</span>
        const h = history.current[m.topic] ?? []
        return (
          <div className="flex items-center gap-2">
            {h.length > 1 && <Spark data={h} color={COLOR.accent} />}
            <span className="font-mono text-[11px] text-muted-foreground">
              {m.eventsPerSec.toFixed(1)}/s
            </span>
          </div>
        )
      },
    }),
    columnHelper.display({
      id: 'jdbcLag',
      header: () => <span title="타깃 DB에 아직 반영되지 않은 이벤트 수">JDBC LAG</span>,
      cell: ({ row }) => {
        const m = row.original.metrics
        if (!m) return <span className="font-mono text-muted-foreground">—</span>
        // 정지 중 lag 증가는 당연한 결과 — 경고색 대신 회색 유지
        const paused = sinkState(row.original) === 'PAUSED'
        return (
          <span
            className={`font-mono ${
              paused
                ? 'text-muted-foreground'
                : m.jdbcLag > 100
                  ? 'text-warn'
                  : 'text-foreground'
            }`}
          >
            {m.jdbcLag.toLocaleString()}
          </span>
        )
      },
    }),
    columnHelper.display({
      id: 'icebergLag',
      header: () => (
        <span title="changelog에 아직 기록되지 않은 이벤트 수 — 커밋 주기(60초)만큼의 지연은 정상">
          ICEBERG LAG
        </span>
      ),
      cell: ({ row }) => {
        const m = row.original.metrics
        if (!m) return <span className="font-mono text-muted-foreground">—</span>
        return (
          <span className={`font-mono ${m.icebergLag > 1000 ? 'text-warn' : 'text-foreground'}`}>
            {m.icebergLag.toLocaleString()}
          </span>
        )
      },
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
        <button
          className="block w-full border-b border-brand/40 bg-brand/10 px-4 py-2.5 text-left text-[13px] hover:bg-brand/15"
          onClick={() => setResnapDialog('routine')}
        >
          <span className="font-semibold text-brand">⟳ 재스냅샷 진행 중</span>
          <span className="ml-2 font-mono text-[12px] text-muted-foreground">
            {RUN_PHASE_LABEL[run!.phase] ?? run!.phase}
            {run!.phase === 'DRAINING' && ` (남은 ${run!.remainingLag.toLocaleString()}건)`}
            {run!.phase === 'SNAPSHOTTING' &&
              ` (${Object.keys(run!.snapshot?.tables ?? {}).length}개 테이블 완료)`}
            {' '}— 클릭하여 상세
          </span>
        </button>
      )}
      {goLiveJustNow && !snapshotActive && (
        <div className="flex items-center border-b border-ok/40 bg-ok/10 px-4 py-2 text-[13px] text-ok">
          <span>
            ✓ go-live — 초기 스냅샷 완료, 스트리밍 재개됨
            <span className="ml-2 font-mono text-[11px] text-muted-foreground">
              {Object.entries(run!.snapshot?.tables ?? {})
                .map(([t, r]) => `${t.split('.').slice(-1)[0]} ${r.toLocaleString()}행`)
                .join(' · ')}
            </span>
          </span>
          <button
            className="ml-auto px-1 text-muted-foreground hover:text-foreground"
            title="배너 닫기"
            onClick={() => setGoLiveDismissed(run!.startedAtMs)}
          >
            ✕
          </button>
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
          {table.getRowModel().rows.length} tables
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

      <ResnapshotDialog
        open={resnapDialog !== null}
        context={resnapDialog ?? 'routine'}
        onClose={() => setResnapDialog(null)}
      />
    </div>
  )
}

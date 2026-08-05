/**
 * 파일명 : ResnapshotDialog.tsx
 * 작성일자 : 26. 08. 05.
 * 작성자 : 최남희
 * 설명 : 재스냅샷 단계 팝업 (등록 위저드 스타일) — 설정 → 시작 후에는 상태 기계의
 * 단계(유입 차단 → 잔량 소진 → 타깃 비우기 승인/홀드 → offset 리셋 → 초기 스냅샷
 * → go-live)를 실시간 표시. 홀드 단계에서 DBA 안내 SQL·테이블별 잔여 행·재검사 제공.
 * 팝업을 닫아도 진행은 backend에서 계속되고, 다시 열면 현재 단계부터 보인다.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 05.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
import { useEffect, useState } from 'react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { api } from '@/lib/api'

export interface RunStatus {
  phase:
    | 'STOPPING_SOURCE'
    | 'DRAINING'
    | 'AWAITING_DECISION'
    | 'HELD'
    | 'TRUNCATING'
    | 'RESETTING'
    | 'SNAPSHOTTING'
    | 'DONE'
    | 'FAILED'
    | 'CANCELLED'
  mode: string
  truncateTarget: boolean
  remainingLag: number
  decision: string | null
  holdReason: string | null
  tableCounts: Record<string, number>
  truncateSql: string[]
  error: string | null
  startedAtMs: number
  finishedAtMs: number | null
  snapshot: {
    phase: string
    currentTable: string | null
    tables: Record<string, number>
  }
}

export const RUN_ACTIVE = (r: RunStatus | null): boolean =>
  r !== null && !['DONE', 'FAILED', 'CANCELLED'].includes(r.phase)

/** 단계 정의 — phase → 몇 번째 단계인지 */
const STEP_OF: Record<string, number> = {
  STOPPING_SOURCE: 0,
  DRAINING: 1,
  AWAITING_DECISION: 2,
  HELD: 2,
  TRUNCATING: 2,
  RESETTING: 3,
  SNAPSHOTTING: 4,
  DONE: 6, // 전 단계 완료
  FAILED: -1,
  CANCELLED: -1,
}

export function ResnapshotDialog({
  open,
  context,
  onClose,
}: {
  open: boolean
  /** 'routine': 헤더 재스냅샷 (INITIAL만) | 'recover': 장애 배너 (NO_DATA 옵션 포함) */
  context: 'routine' | 'recover'
  onClose: () => void
}) {
  const [run, setRun] = useState<RunStatus | null>(null)
  const [mode, setMode] = useState<'INITIAL' | 'NO_DATA'>('INITIAL')
  const [truncate, setTruncate] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open) return
    const poll = () =>
      api<RunStatus | undefined>('/api/capture/resnapshot/run')
        .then((r) => setRun(r ?? null))
        .catch(() => {})
    poll()
    const id = setInterval(poll, 2000)
    return () => clearInterval(id)
  }, [open])

  const call = async (path: string, body?: unknown) => {
    setBusy(true)
    setError(null)
    try {
      await api(path, { method: 'POST', body: body ? JSON.stringify(body) : undefined })
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setBusy(false)
    }
  }

  const start = () =>
    call('/api/capture/resnapshot', {
      mode,
      truncateTarget: mode === 'INITIAL' && truncate,
    })

  const showRun = RUN_ACTIVE(run) || run?.phase === 'DONE' || run?.phase === 'FAILED' || run?.phase === 'CANCELLED'
  // 종료된 run이라도 방금(2분 내) 것만 보여주고, 오래된 건 설정 화면으로
  const finishedRecently = run?.finishedAtMs != null && Date.now() - run.finishedAtMs < 120_000
  const view = RUN_ACTIVE(run) || (showRun && finishedRecently) ? 'run' : 'config'

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="sm:max-w-xl">
        {view === 'config' ? (
          <>
            <DialogHeader>
              <DialogTitle>{context === 'recover' ? '캡처 복구' : '재스냅샷'}</DialogTitle>
              <DialogDescription>
                캡처 커넥터를 offset 리셋 후 재기동합니다 (전 테이블 대상). 시작 후 각 단계가
                이 팝업에 순서대로 표시되고, 닫아도 진행은 계속됩니다.
              </DialogDescription>
            </DialogHeader>
            {context === 'recover' && (
              <div className="flex flex-col gap-2 text-sm">
                <label className="flex items-start gap-2">
                  <input type="radio" className="mt-1" checked={mode === 'INITIAL'}
                    onChange={() => setMode('INITIAL')} />
                  <span>
                    <b>초기 스냅샷부터</b> (권장)
                    <span className="block text-xs text-muted-foreground">
                      전 테이블 재적재 후 스트리밍 — 유실 없이 현재 상태로 재구축합니다.
                    </span>
                  </span>
                </label>
                <label className="flex items-start gap-2">
                  <input type="radio" className="mt-1" checked={mode === 'NO_DATA'}
                    onChange={() => setMode('NO_DATA')} />
                  <span>
                    <b>현재 시점부터</b> (갭 유실 수용)
                    <span className="block text-xs text-crit">
                      장애 시점부터 지금까지의 변경은 영구 유실됩니다.
                    </span>
                  </span>
                </label>
              </div>
            )}
            {mode === 'INITIAL' && (
              <label className="flex items-start gap-2 text-sm">
                <input type="checkbox" className="mt-0.5" checked={truncate}
                  onChange={(e) => setTruncate(e.target.checked)} />
                <span>
                  타깃을 비우고 재적재 (완전 재구축)
                  <span className="block text-xs text-crit">
                    소스에서 삭제된 행(고아)까지 정리되는 유일한 방법. TRUNCATE 실행 주체는
                    다음 단계에서 선택하며(시스템/직접·DBA), 스냅샷 완료까지 타깃 조회가 비어
                    보입니다.
                  </span>
                </span>
              </label>
            )}
            {error && <p className="text-sm text-destructive">{error}</p>}
            <DialogFooter>
              <Button variant="ghost" onClick={onClose}>취소</Button>
              <Button variant={truncate ? 'destructive' : 'default'} disabled={busy} onClick={start}>
                {mode === 'NO_DATA' ? '현재 시점부터 재개'
                  : truncate ? '타깃 비우고 초기 스냅샷 시작' : '초기 스냅샷 시작'}
              </Button>
            </DialogFooter>
          </>
        ) : (
          <RunView run={run!} busy={busy} error={error} call={call} onClose={onClose} />
        )}
      </DialogContent>
    </Dialog>
  )
}

function RunView({
  run, busy, error, call, onClose,
}: {
  run: RunStatus
  busy: boolean
  error: string | null
  call: (path: string, body?: unknown) => Promise<void>
  onClose: () => void
}) {
  const cur = STEP_OF[run.phase]
  const scanned = Object.values(run.snapshot?.tables ?? {}).reduce((a, b) => a + b, 0)
  const steps: { name: string; detail?: string; skip?: boolean }[] = [
    { name: '유입 차단', detail: 'source 정지 (offset 보존)' },
    {
      name: '파이프 잔량 소진',
      detail: run.phase === 'DRAINING' ? `남은 이벤트 ${run.remainingLag.toLocaleString()}건` : undefined,
      skip: !run.truncateTarget,
    },
    { name: '타깃 비우기', skip: !run.truncateTarget },
    { name: 'offset 리셋 · 재배포' },
    {
      name: '초기 스냅샷',
      detail:
        run.phase === 'SNAPSHOTTING'
          ? `${Object.keys(run.snapshot?.tables ?? {}).length}개 테이블 완료 (${scanned.toLocaleString()}행)` +
            (run.snapshot?.currentTable ? ` · 진행 중: ${run.snapshot.currentTable}` : '')
          : run.phase === 'DONE'
            ? `${scanned.toLocaleString()}행`
            : undefined,
      skip: run.mode === 'NO_DATA',
    },
    { name: 'go-live (스트리밍 전환)' },
  ]

  const mark = (i: number) => {
    if (run.phase === 'FAILED' || run.phase === 'CANCELLED') return i < 99 ? '·' : '·'
    if (steps[i].skip) return '—'
    if (cur > i || run.phase === 'DONE') return '✓'
    if (cur === i) return '⟳'
    return '○'
  }
  const color = (i: number) => {
    if (steps[i].skip) return 'text-muted-foreground/50'
    if (cur > i || run.phase === 'DONE') return 'text-ok'
    if (cur === i) return 'text-[#53C8E8]'
    return 'text-muted-foreground'
  }

  const cancellable = ['STOPPING_SOURCE', 'DRAINING', 'AWAITING_DECISION', 'HELD'].includes(run.phase)

  return (
    <>
      <DialogHeader>
        <DialogTitle>
          {run.phase === 'DONE' ? '재스냅샷 완료'
            : run.phase === 'FAILED' ? '재스냅샷 실패'
              : run.phase === 'CANCELLED' ? '재스냅샷 취소됨'
                : '재스냅샷 진행 중'}
        </DialogTitle>
        {run.phase === 'FAILED' && (
          <DialogDescription className="text-crit">{run.error}</DialogDescription>
        )}
        {run.phase === 'CANCELLED' && (
          <DialogDescription>취소 — source는 기존 offset부터 재개되었습니다 (원복).</DialogDescription>
        )}
      </DialogHeader>

      <ol className="flex flex-col gap-1.5 text-sm">
        {steps.map((s, i) => (
          <li key={s.name} className={`flex items-baseline gap-2 ${color(i)}`}>
            <span className="w-4 font-mono">{mark(i)}</span>
            <span className={cur === i && !s.skip ? 'font-semibold' : ''}>{s.name}</span>
            {s.skip && <span className="text-[11px]">(생략)</span>}
            {s.detail && !s.skip && (
              <span className="font-mono text-[11px] text-muted-foreground">{s.detail}</span>
            )}
          </li>
        ))}
      </ol>

      {run.phase === 'AWAITING_DECISION' && (
        <div className="rounded-md border border-border bg-surface2 p-3 text-sm">
          <p className="mb-2">
            타깃 테이블 TRUNCATE를 <b>누가 실행합니까?</b>
          </p>
          <div className="flex gap-2">
            <Button size="sm" disabled={busy}
              onClick={() => call('/api/capture/resnapshot/decision', { choice: 'SYSTEM' })}>
              시스템이 실행
            </Button>
            <Button size="sm" variant="outline" disabled={busy}
              onClick={() => call('/api/capture/resnapshot/decision', { choice: 'MANUAL' })}>
              직접/DBA가 실행
            </Button>
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            시스템 실행 선택 시 권한(스키마 소유 또는 DROP ANY TABLE)을 먼저 점검하고,
            불충분하면 아래 홀드 상태로 전환됩니다.
          </p>
        </div>
      )}

      {run.phase === 'HELD' && (
        <div className="rounded-md border border-warn/40 bg-warn/10 p-3 text-sm">
          <p className="font-semibold text-warn">⏸ 홀드 — {run.holdReason}</p>
          <pre className="mt-2 overflow-x-auto rounded bg-surface2 p-2 text-[11px] leading-relaxed">
            {run.truncateSql.join('\n')}
          </pre>
          <div className="mt-2 font-mono text-[11px] text-muted-foreground">
            {Object.entries(run.tableCounts).map(([t, c]) => (
              <div key={t}>
                {t}: {c.toLocaleString()}행 {c === 0 ? '✓' : '(0이 되면 자동 진행)'}
              </div>
            ))}
          </div>
          <Button size="sm" variant="outline" className="mt-2" disabled={busy}
            onClick={() => call('/api/capture/resnapshot/recheck')}>
            지금 검사
          </Button>
          <p className="mt-1 text-[11px] text-muted-foreground">10초마다 자동 재검사합니다.</p>
        </div>
      )}

      {run.phase === 'DONE' && (
        <p className="text-sm text-ok">✓ 스트리밍 재개 — 이후 정합 검증(복구 화면)으로 일치를 확인하세요.</p>
      )}
      {error && <p className="text-sm text-destructive">{error}</p>}

      <DialogFooter>
        {cancellable && (
          <Button variant="ghost" disabled={busy}
            onClick={() => call('/api/capture/resnapshot/cancel')}>
            실행 취소 (원복)
          </Button>
        )}
        <Button variant="outline" onClick={onClose}>닫기</Button>
      </DialogFooter>
    </>
  )
}

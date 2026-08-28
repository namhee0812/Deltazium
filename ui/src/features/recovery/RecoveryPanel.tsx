/**
 * 파일명 : RecoveryPanel.tsx
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 복구 화면 — changelog(S3) 현황, SCN 재발행 트리거, go-live 자동 재개, 정합 검증.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 28.       | 최남희  | 리디자인 — 페이지는 changelog 현황·실행 이력(카드)만 남기고,
 * |                          | SCN 재발행 트리거 폼을 RecoveryDrawer.dc.html 구조의 우측
 * |                          | drawer(진한 헤더·단계 카드 3개·실행 요약·하단 실행 바)로 분리.
 * |                          | API·검증 로직은 그대로 — 재발행 범위는 backend가 지원하는
 * |                          | SCN 모드만 활성화하고 시점/전체 changelog 모드는 "준비 중"으로
 * |                          | 비활성 표시(신규 API 없이는 구현할 수 없어 임의 추가하지 않음).
 * --------------------------------------------------
 */
import { useCallback, useEffect, useState } from 'react'
import { X } from 'lucide-react'
import { api } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { GhostButton } from '@/components/ui/ghost-button'
import { Input } from '@/components/ui/input'
import { StatusPill } from '@/components/ui/status-pill'
import type { StatusPillVariant } from '@/components/ui/status-pill'

/* 복구 (architecture.md 6절) — SCN 지정 재발행 트리거 + 정합 검증(행수·체크섬).
   recovery-job은 재발행까지만, apply는 recovery-sink(live와 동일 설정)가 담당 */

interface RegisteredTable {
  id: number
  schemaName: string
  tableName: string
}

interface RecoveryRun {
  id: number
  table: string
  fromScn: number
  status: 'RUNNING' | 'DONE' | 'APPLIED' | 'LIVE' | 'FAILED'
  published: number
  skipped: number
  logPath: string
  startedAt: string
}

interface VerifyResult {
  sourceCount: number
  targetCount: number
  sourceChecksum: number
  targetChecksum: number
  match: boolean
}

interface PartitionInfo {
  day: string
  records: number
  files: number
  bytes: number
}

interface ChangelogInfo {
  table: string
  totalRecords: number
  totalFiles: number
  totalBytes: number
  snapshotCount: number
  lastCommitAtMs: number | null
  partitions: PartitionInfo[]
}

const fmtBytes = (n: number) =>
  n >= 1 << 30
    ? `${(n / (1 << 30)).toFixed(1)} GB`
    : n >= 1 << 20
      ? `${(n / (1 << 20)).toFixed(1)} MB`
      : `${(n / 1024).toFixed(1)} KB`

const STATUS_VARIANT: Record<RecoveryRun['status'], StatusPillVariant> = {
  RUNNING: 'warn',
  DONE: 'ok',
  APPLIED: 'ok',
  LIVE: 'ok',
  FAILED: 'crit',
}

const statusLabel: Record<RecoveryRun['status'], string> = {
  RUNNING: '재발행 중',
  DONE: 'apply 대기',
  APPLIED: '적용 완료',
  LIVE: 'go-live 완료',
  FAILED: '실패',
}

export function RecoveryPanel() {
  const [tables, setTables] = useState<RegisteredTable[]>([])
  const [selected, setSelected] = useState<string>('')
  const [fromScn, setFromScn] = useState('')
  const [autoResume, setAutoResume] = useState(true)
  const [runs, setRuns] = useState<RecoveryRun[]>([])
  const [verify, setVerify] = useState<VerifyResult | null>(null)
  const [changelog, setChangelog] = useState<ChangelogInfo[] | null>(null)
  const [changelogError, setChangelogError] = useState<string | null>(null)
  const [openTable, setOpenTable] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [drawerOpen, setDrawerOpen] = useState(false)

  const loadChangelog = useCallback(() => {
    setChangelogError(null)
    // 카탈로그 조회는 수백 ms — 주기 폴링 없이 진입/새로고침 시에만
    api<ChangelogInfo[]>('/api/changelog')
      .then(setChangelog)
      .catch((e: Error) => setChangelogError(e.message))
  }, [])

  const loadRuns = useCallback(() => {
    api<RecoveryRun[]>('/api/recovery').then(setRuns).catch(() => setRuns([]))
  }, [])

  useEffect(() => {
    api<RegisteredTable[]>('/api/registrations').then(setTables).catch(() => setTables([]))
    loadRuns()
    loadChangelog()
    const id = setInterval(loadRuns, 5000)
    return () => clearInterval(id)
  }, [loadRuns, loadChangelog])

  const run = async (fn: () => Promise<void>) => {
    setBusy(true)
    setError(null)
    try {
      await fn()
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setBusy(false)
    }
  }

  const trigger = () =>
    run(async () => {
      await api('/api/recovery', {
        method: 'POST',
        body: JSON.stringify({
          registeredTableId: Number(selected),
          fromScn: Number(fromScn),
          autoResume,
        }),
      })
      loadRuns()
      setDrawerOpen(false)
    })

  const doVerify = () =>
    run(async () => {
      setVerify(null)
      setVerify(
        await api<VerifyResult>('/api/recovery/verify', {
          method: 'POST',
          body: JSON.stringify({ registeredTableId: Number(selected) }),
        }),
      )
    })

  const selectedTable = tables.find((t) => String(t.id) === selected)

  return (
    <div className="flex h-full min-h-0">
      <div className="flex min-w-0 flex-1 flex-col gap-4 overflow-y-auto p-5">
        <div className="flex items-center justify-between">
          <div className="text-[13px] font-semibold text-foreground">changelog(S3) 현황 · 복구 실행 이력</div>
          <Button onClick={() => setDrawerOpen(true)}>복구 실행</Button>
        </div>

        {verify && (
          <div className="rounded-lg border border-border bg-card shadow-[var(--shadow-card)]">
            <div className="flex items-center gap-2 border-b border-border px-4 py-2.5">
              <span className="text-[13px] font-semibold">정합 검증 결과</span>
              <StatusPill variant={verify.match ? 'ok' : 'crit'} className="ml-auto">
                {verify.match ? '일치' : '불일치'}
              </StatusPill>
            </div>
            <div className="grid grid-cols-2 gap-2 px-4 py-3 font-mono text-[12px]">
              <div>SRC rows: {verify.sourceCount.toLocaleString()}</div>
              <div>TGT rows: {verify.targetCount.toLocaleString()}</div>
              <div>SRC checksum: {verify.sourceChecksum}</div>
              <div>TGT checksum: {verify.targetChecksum}</div>
            </div>
          </div>
        )}

        <div className="rounded-lg border border-border bg-card shadow-[var(--shadow-card)]">
          <div className="flex items-center gap-2 border-b border-border px-4 py-2.5">
            <span className="text-[13px] font-semibold">changelog 현황</span>
            <span className="text-xs text-ink-3">복구 가능 범위 — 파티션(1일)이 남아 있는 구간까지</span>
            <GhostButton className="ml-auto" onClick={loadChangelog}>새로고침</GhostButton>
          </div>
          <div className="p-3">
            {changelogError && <p className="px-1 py-1 text-sm text-destructive">{changelogError}</p>}
            {changelog !== null && changelog.length === 0 && !changelogError && (
              <p className="px-1 py-1 text-sm text-ink-3">changelog 테이블이 없습니다.</p>
            )}
            <div className="grid gap-1.5">
              {(changelog ?? []).map((c) => {
                const isOpen = openTable === c.table
                const range =
                  c.partitions.length > 0
                    ? `${c.partitions[0].day} ~ ${c.partitions[c.partitions.length - 1].day}`
                    : '—'
                return (
                  <div key={c.table}>
                    <div
                      onClick={() => setOpenTable(isOpen ? null : c.table)}
                      className={`flex cursor-pointer flex-wrap items-center gap-3 rounded-md border px-3 py-2 ${
                        isOpen ? 'border-primary' : 'border-border'
                      }`}
                    >
                      <span className="font-mono text-[12px] font-semibold">{c.table}</span>
                      <span className="font-mono text-[11px] text-ink-3">
                        {c.totalRecords.toLocaleString()} rec · {c.totalFiles} files · {fmtBytes(c.totalBytes)}
                      </span>
                      <span className="font-mono text-[11px] text-ink-3">{range}</span>
                      <span className="ml-auto font-mono text-[10.5px] text-ink-3">
                        {c.lastCommitAtMs
                          ? `마지막 커밋 ${new Date(c.lastCommitAtMs).toLocaleString('sv-SE').slice(5, 16)}`
                          : '커밋 없음'}
                      </span>
                    </div>
                    {isOpen && c.partitions.length > 0 && (
                      <table className="ml-3 mt-1 text-[11.5px]">
                        <tbody>
                          {c.partitions.map((p) => (
                            <tr key={p.day} className="font-mono text-ink-3">
                              <td className="pr-4">{p.day}</td>
                              <td className="pr-4 text-right">{p.records.toLocaleString()} rec</td>
                              <td className="pr-4 text-right">{p.files} files</td>
                              <td className="text-right">{fmtBytes(p.bytes)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                )
              })}
            </div>
          </div>
        </div>

        <div className="rounded-lg border border-border bg-card shadow-[var(--shadow-card)]">
          <div className="border-b border-border px-4 py-2.5 text-[13px] font-semibold">복구 실행 이력</div>
          <div className="p-3">
            {runs.length === 0 && <p className="px-1 py-1 text-sm text-ink-3">실행 이력이 없습니다.</p>}
            <div className="grid gap-2">
              {runs.map((r) => (
                <div key={r.id} className="flex flex-wrap items-center gap-3 rounded-md border border-border px-3.5 py-2.5">
                  <span className="font-mono text-[12.5px] font-semibold">{r.table}</span>
                  <span className="font-mono text-[11px] text-ink-3">from SCN {r.fromScn}</span>
                  <StatusPill variant={STATUS_VARIANT[r.status]}>{statusLabel[r.status] ?? r.status}</StatusPill>
                  {r.status !== 'RUNNING' && (
                    <span className="font-mono text-[11px] text-ink-3">
                      발행 {r.published.toLocaleString()}건{r.skipped > 0 && ` · 건너뜀 ${r.skipped}`}
                    </span>
                  )}
                  <span className="ml-auto rounded border border-line-2 px-2 py-0.5 font-mono text-[10px] text-ink-3">
                    {r.logPath.split('/').pop()}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {drawerOpen && (
        <RecoveryDrawer
          tables={tables}
          selected={selected}
          setSelected={setSelected}
          fromScn={fromScn}
          setFromScn={setFromScn}
          autoResume={autoResume}
          setAutoResume={setAutoResume}
          selectedTable={selectedTable}
          busy={busy}
          error={error}
          onTrigger={() => void trigger()}
          onVerify={() => void doVerify()}
          verify={verify}
          onClose={() => setDrawerOpen(false)}
        />
      )}
    </div>
  )
}

/* 복구 실행 drawer — RecoveryDrawer.dc.html 구조(진한 헤더·단계 카드 3개·실행 요약·
   하단 실행 바)를 그대로 이식. 재발행 범위는 backend가 지원하는 SCN 모드만 활성화한다 —
   시점(timestamp)·전체 changelog 모드는 SCN 환산 API가 없어 "준비 중"으로 비활성 표시. */
function RecoveryDrawer({
  tables,
  selected,
  setSelected,
  fromScn,
  setFromScn,
  autoResume,
  setAutoResume,
  selectedTable,
  busy,
  error,
  onTrigger,
  onVerify,
  verify,
  onClose,
}: {
  tables: RegisteredTable[]
  selected: string
  setSelected: (v: string) => void
  fromScn: string
  setFromScn: (v: string) => void
  autoResume: boolean
  setAutoResume: (v: boolean) => void
  selectedTable: RegisteredTable | undefined
  busy: boolean
  error: string | null
  onTrigger: () => void
  onVerify: () => void
  verify: VerifyResult | null
  onClose: () => void
}) {
  const canTrigger = !!selected && !!fromScn && !busy

  return (
    <div className="flex w-[480px] shrink-0 flex-col border-l border-border bg-background shadow-[-12px_0_32px_rgba(16,24,40,.12)]">
      <div className="flex items-center gap-2.5 bg-rail px-5 py-4 text-rail-ink">
        <div className="flex flex-wrap items-baseline gap-2.5">
          <span className="text-[15px] font-semibold">복구 실행</span>
          <span className="text-xs opacity-75">Iceberg changelog → 복구 토픽 재발행</span>
        </div>
        <button className="ml-auto text-rail-ink-2 hover:text-rail-ink" onClick={onClose} title="닫기">
          <X className="size-4.5" />
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-3.5">
        {/* 1. 대상 테이블 */}
        <div className="mb-3 rounded-lg border border-border bg-card shadow-[var(--shadow-card)]">
          <div className="flex items-center gap-2 border-l-4 border-l-primary bg-surface-2 px-3.5 py-2.5">
            <span className="text-[13.5px] font-bold">1 · 대상 테이블</span>
            <span className="ml-auto text-[11px] font-semibold text-ink-3">
              {selectedTable ? `${selectedTable.schemaName}.${selectedTable.tableName}` : '미선택'}
            </span>
          </div>
          <div className="flex flex-wrap gap-1.5 p-3.5">
            {tables.map((t) => {
              const on = String(t.id) === selected
              return (
                <button
                  key={t.id}
                  onClick={() => setSelected(String(t.id))}
                  className={`inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs ${
                    on ? 'border-primary bg-brand-soft text-primary' : 'border-border bg-card text-foreground'
                  }`}
                >
                  <span className={`font-mono text-[10.5px] ${on ? 'text-primary' : 'text-ink-3'}`}>
                    {t.schemaName}
                  </span>
                  {t.tableName}
                </button>
              )
            })}
            {tables.length === 0 && <p className="text-xs text-ink-3">등록된 테이블이 없습니다.</p>}
          </div>
        </div>

        {/* 2. 범위 */}
        <div className="mb-3 rounded-lg border border-border bg-card shadow-[var(--shadow-card)]">
          <div className="flex items-center gap-2 border-l-4 border-l-primary bg-surface-2 px-3.5 py-2.5">
            <span className="text-[13.5px] font-bold">2 · 재발행 범위</span>
            <span className="ml-auto text-[11px] font-semibold text-ink-3">SCN 범위</span>
          </div>
          <div className="flex flex-wrap items-center gap-1.5 px-3.5 pt-2.5">
            <span className="inline-flex items-center rounded-full border border-primary bg-primary px-3 py-1 text-xs font-semibold text-primary-foreground">
              SCN 범위
            </span>
            <span className="inline-flex cursor-not-allowed items-center rounded-full border border-line-2 px-3 py-1 text-xs font-semibold text-ink-3 opacity-45" title="아직 지원하지 않음">
              시점 (timestamp)
            </span>
            <span className="inline-flex cursor-not-allowed items-center rounded-full border border-line-2 px-3 py-1 text-xs font-semibold text-ink-3 opacity-45" title="아직 지원하지 않음">
              전체 changelog
            </span>
          </div>
          <div className="flex flex-col gap-2 p-3.5">
            <div className="flex items-center gap-2 text-xs">
              <span className="w-9 text-ink-3">from</span>
              <Input
                value={fromScn}
                onChange={(e) => setFromScn(e.target.value.replace(/[^0-9]/g, ''))}
                placeholder="시작 SCN"
                className="w-40 font-mono"
              />
              <span className="text-ink-3">이 값부터 재발행</span>
            </div>
            <label className="flex cursor-pointer items-start gap-2 pt-1 text-xs">
              <input
                type="checkbox"
                className="mt-0.5"
                checked={autoResume}
                onChange={(e) => setAutoResume(e.target.checked)}
              />
              <span>
                <b>복구 완료 후 자동 재개 (go-live)</b>
                <span className="block text-ink-3">
                  apply 완료(lag 0)가 확인되면 이 테이블의 jdbc-sink를 자동으로 재개합니다.
                </span>
              </span>
            </label>
          </div>
        </div>

        {/* 3. 검증 */}
        <div className="mb-3 rounded-lg border border-border bg-card shadow-[var(--shadow-card)]">
          <div className="flex items-center gap-2 border-l-4 border-l-warn bg-surface-2 px-3.5 py-2.5">
            <span className="text-[13.5px] font-bold">3 · 정합 검증</span>
            <span className="ml-auto text-[11px] font-semibold text-warn">수동 실행</span>
          </div>
          <div className="flex flex-col gap-2.5 p-3.5 text-[12.5px]">
            <p className="text-ink-3">
              source vs target 행 수·체크섬(ORA_HASH) 비교 — 대형 테이블은 소요 시간이 클 수 있습니다.
              재발행 apply가 끝난 뒤 실행하세요.
            </p>
            <GhostButton disabled={!selected || busy} onClick={onVerify} className="w-fit">
              정합 검증 실행
            </GhostButton>
            {verify && (
              <div className={`rounded-md border px-3 py-2 font-mono text-[11.5px] ${
                verify.match ? 'border-ok/40 bg-ok-soft text-ok' : 'border-crit/40 bg-crit-soft text-crit'
              }`}>
                {verify.match ? '일치' : '불일치'} · SRC {verify.sourceCount.toLocaleString()} / TGT{' '}
                {verify.targetCount.toLocaleString()}
              </div>
            )}
          </div>
        </div>

        {error && <p className="text-sm text-destructive">{error}</p>}
      </div>

      {/* 실행 요약 */}
      <div className="max-h-[150px] overflow-auto border-t border-border bg-card px-4 py-2.5">
        <div className="mb-1.5 text-[11px] font-semibold uppercase tracking-wide text-ink-3">실행 요약</div>
        {selectedTable && fromScn ? (
          <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-[12.5px]">
            <span className="whitespace-nowrap text-foreground">
              <span className="font-mono text-[11px] text-ink-3">{selectedTable.schemaName}</span>{' '}
              {selectedTable.tableName}
            </span>
            <span className="text-ink-2">SCN {fromScn} → 현재{autoResume ? ' · go-live 자동 재개' : ''}</span>
            <span className="text-ink-3">복구 토픽</span>
            <span className="font-mono text-[12px] text-ink-2">recovery-sink (동일 JDBC 설정)</span>
          </div>
        ) : (
          <p className="text-xs text-ink-3">대상 테이블과 시작 SCN을 지정하세요.</p>
        )}
      </div>

      {/* 하단 실행 바 */}
      <div className="flex items-center gap-2 border-t border-border bg-surface-2 px-4 py-3">
        <span className="mr-auto max-w-[30ch] text-[11.5px] text-ink-3">
          PK upsert 멱등 — 중복 재발행은 안전, at-least-once
        </span>
        <GhostButton className="h-9 px-4" onClick={onClose}>취소</GhostButton>
        <Button disabled={!canTrigger} onClick={onTrigger} className="h-9 px-5">
          재발행 시작
        </Button>
      </div>
    </div>
  )
}

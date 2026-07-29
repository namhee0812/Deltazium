import { useCallback, useEffect, useState } from 'react'
import { api } from '@/lib/api'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

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
  status: 'RUNNING' | 'DONE' | 'APPLIED' | 'FAILED'
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

const statusBadge: Record<RecoveryRun['status'], string> = {
  RUNNING: 'text-warn bg-warn/10',
  DONE: 'text-ok bg-ok/10',
  APPLIED: 'text-ok bg-ok/10',
  FAILED: 'text-crit bg-crit/10',
}

const statusLabel: Record<RecoveryRun['status'], string> = {
  RUNNING: '재발행 중',
  DONE: 'apply 대기',
  APPLIED: '적용 완료',
  FAILED: '실패',
}

export function RecoveryPanel() {
  const [tables, setTables] = useState<RegisteredTable[]>([])
  const [selected, setSelected] = useState<string>('')
  const [fromScn, setFromScn] = useState('')
  const [runs, setRuns] = useState<RecoveryRun[]>([])
  const [verify, setVerify] = useState<VerifyResult | null>(null)
  const [changelog, setChangelog] = useState<ChangelogInfo[] | null>(null)
  const [changelogError, setChangelogError] = useState<string | null>(null)
  const [openTable, setOpenTable] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

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
        body: JSON.stringify({ registeredTableId: Number(selected), fromScn: Number(fromScn) }),
      })
      loadRuns()
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

  return (
    <div className="mx-auto max-w-3xl space-y-5 p-6">
      <div className="rounded-[10px] border border-border bg-card p-4">
        <div className="mb-3 text-[13px] font-semibold">SCN 지정 재발행</div>
        <div className="flex flex-wrap items-center gap-2">
          <Select value={selected} onValueChange={setSelected}>
            <SelectTrigger className="w-56">
              <SelectValue placeholder="등록 테이블 선택" />
            </SelectTrigger>
            <SelectContent>
              {tables.map((t) => (
                <SelectItem key={t.id} value={String(t.id)}>
                  {t.schemaName}.{t.tableName}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Input
            value={fromScn}
            onChange={(e) => setFromScn(e.target.value.replace(/[^0-9]/g, ''))}
            placeholder="시작 SCN (이 값부터 재발행)"
            className="w-64 font-mono"
          />
          <Button disabled={!selected || !fromScn || busy} onClick={() => void trigger()}>
            복구 실행
          </Button>
          <Button
            variant="outline"
            disabled={!selected || busy}
            onClick={() => void doVerify()}
          >
            정합 검증
          </Button>
        </div>
        <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
          Iceberg changelog에서 SCN ≥ 시작값을 순서 복원해 복구 토픽으로 재발행하고,
          recovery-sink(live와 동일 apply 설정)가 타깃에 적용합니다. 경계 중복은 PK upsert
          멱등으로 안전합니다.
        </p>
        {error && <p className="mt-2 text-sm text-destructive">{error}</p>}
      </div>

      {verify && (
        <div
          className={`rounded-[10px] border p-4 ${
            verify.match ? 'border-ok bg-ok/10' : 'border-crit bg-crit/10'
          }`}
        >
          <div className="mb-2 text-[13px] font-semibold">
            정합 검증 {verify.match ? '일치 ✓' : '불일치 ✕'}
          </div>
          <div className="grid grid-cols-2 gap-2 font-mono text-[12px]">
            <div>SRC rows: {verify.sourceCount.toLocaleString()}</div>
            <div>TGT rows: {verify.targetCount.toLocaleString()}</div>
            <div>SRC checksum: {verify.sourceChecksum}</div>
            <div>TGT checksum: {verify.targetChecksum}</div>
          </div>
        </div>
      )}

      <div className="rounded-[10px] border border-border bg-card p-4">
        <div className="mb-1 flex items-center justify-between">
          <div className="text-[13px] font-semibold">changelog(S3) 현황 — 복구 가능 범위</div>
          <Button variant="outline" size="sm" onClick={loadChangelog}>
            새로고침
          </Button>
        </div>
        <p className="mb-3 text-xs text-muted-foreground">
          Iceberg 메타데이터 기준. 파티션(1일)이 남아 있는 구간까지 SCN 재발행 복구가 가능합니다.
        </p>
        {changelogError && <p className="text-sm text-destructive">{changelogError}</p>}
        {changelog !== null && changelog.length === 0 && !changelogError && (
          <p className="text-sm text-muted-foreground">changelog 테이블이 없습니다.</p>
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
                  className={`flex cursor-pointer flex-wrap items-center gap-3 rounded-[9px] border px-3 py-2 ${
                    isOpen ? 'border-primary' : 'border-border'
                  }`}
                >
                  <span className="font-mono text-[12px] font-semibold">{c.table}</span>
                  <span className="font-mono text-[11px] text-muted-foreground">
                    {c.totalRecords.toLocaleString()} rec · {c.totalFiles} files ·{' '}
                    {fmtBytes(c.totalBytes)}
                  </span>
                  <span className="font-mono text-[11px] text-muted-foreground">{range}</span>
                  <span className="ml-auto font-mono text-[10.5px] text-muted-foreground">
                    {c.lastCommitAtMs
                      ? `마지막 커밋 ${new Date(c.lastCommitAtMs)
                          .toLocaleString('sv-SE')
                          .slice(5, 16)}`
                      : '커밋 없음'}
                  </span>
                </div>
                {isOpen && c.partitions.length > 0 && (
                  <table className="ml-3 mt-1 text-[11.5px]">
                    <tbody>
                      {c.partitions.map((p) => (
                        <tr key={p.day} className="font-mono text-muted-foreground">
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

      <div>
        <div className="mb-2 text-[13px] font-semibold">복구 실행 이력</div>
        {runs.length === 0 && (
          <p className="text-sm text-muted-foreground">실행 이력이 없습니다.</p>
        )}
        <div className="grid gap-2">
          {runs.map((r) => (
            <div
              key={r.id}
              className="flex flex-wrap items-center gap-3 rounded-[10px] border border-border bg-card px-4 py-2.5"
            >
              <span className="font-mono text-[12.5px] font-semibold">{r.table}</span>
              <span className="font-mono text-[11px] text-muted-foreground">
                from SCN {r.fromScn}
              </span>
              <span className={`rounded px-2 py-0.5 font-mono text-[10.5px] ${statusBadge[r.status]}`}>
                {statusLabel[r.status] ?? r.status}
              </span>
              {r.status !== 'RUNNING' && (
                <span className="font-mono text-[11px] text-muted-foreground">
                  발행 {r.published.toLocaleString()}건
                  {r.skipped > 0 && ` · 건너뜀 ${r.skipped}`}
                </span>
              )}
              <Badge variant="outline" className="ml-auto font-mono text-[10px]">
                {r.logPath.split('/').pop()}
              </Badge>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

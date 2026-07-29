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
  status: 'RUNNING' | 'DONE' | 'FAILED'
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

const statusBadge: Record<RecoveryRun['status'], string> = {
  RUNNING: 'text-warn bg-warn/10',
  DONE: 'text-ok bg-ok/10',
  FAILED: 'text-crit bg-crit/10',
}

export function RecoveryPanel() {
  const [tables, setTables] = useState<RegisteredTable[]>([])
  const [selected, setSelected] = useState<string>('')
  const [fromScn, setFromScn] = useState('')
  const [runs, setRuns] = useState<RecoveryRun[]>([])
  const [verify, setVerify] = useState<VerifyResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const loadRuns = useCallback(() => {
    api<RecoveryRun[]>('/api/recovery').then(setRuns).catch(() => setRuns([]))
  }, [])

  useEffect(() => {
    api<RegisteredTable[]>('/api/registrations').then(setTables).catch(() => setTables([]))
    loadRuns()
    const id = setInterval(loadRuns, 5000)
    return () => clearInterval(id)
  }, [loadRuns])

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
                {r.status}
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

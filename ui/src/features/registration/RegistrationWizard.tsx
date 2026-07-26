import { useEffect, useState } from 'react'
import { api } from '@/lib/api'
import type { DbConnection } from '@/features/connections/types'
import { Badge } from '@/components/ui/badge'
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

/* CDC 등록 위저드 (ui-reference v3의 5단계 구성, backend 실연동)
   정책: PK 없는 테이블 선택 불가(A안) · supp.log는 "적용하겠습니까?" YES일 때만 DDL 시도 */

interface SourceTableInfo {
  schema: string
  table: string
  hasPk: boolean
  suppLogAll: boolean
  numRows: number | null
}

const STEPS = ['소스', '테이블', '타깃', '사전 점검', '검토·배포'] as const

const qualified = (t: SourceTableInfo) => `${t.schema}.${t.table}`

export function RegistrationWizard({
  open,
  onClose,
  onRegistered,
}: {
  open: boolean
  onClose: () => void
  onRegistered: () => void
}) {
  const [step, setStep] = useState(0)
  const [connections, setConnections] = useState<DbConnection[]>([])
  const [sourceId, setSourceId] = useState<number | null>(null)
  const [targetId, setTargetId] = useState<number | null>(null)

  const [pattern, setPattern] = useState('')
  const [candidates, setCandidates] = useState<SourceTableInfo[] | null>(null)
  const [picked, setPicked] = useState<string[]>([])

  const [dbChecks, setDbChecks] = useState<Record<string, string> | null>(null)
  const [suppResults, setSuppResults] = useState<Record<string, string> | null>(null)

  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open) return
    setStep(0)
    setSourceId(null)
    setTargetId(null)
    setPattern('')
    setCandidates(null)
    setPicked([])
    setDbChecks(null)
    setSuppResults(null)
    setError(null)
    api<DbConnection[]>('/api/connections').then(setConnections).catch(() => setConnections([]))
  }, [open])

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

  const discover = () =>
    run(async () => {
      const list = await api<SourceTableInfo[]>(
        `/api/registrations/discover/${sourceId}?pattern=${encodeURIComponent(pattern)}`,
      )
      setCandidates(list)
      setPicked([])
    })

  const loadChecks = () =>
    run(async () => {
      setDbChecks(await api<Record<string, string>>(`/api/registrations/db-checks/${sourceId}`))
    })

  const pickedInfos = (candidates ?? []).filter((t) => picked.includes(qualified(t)))
  const needSupp = pickedInfos.filter(
    (t) => !t.suppLogAll && suppResults?.[qualified(t)] !== 'OK',
  )

  const applySupp = () =>
    run(async () => {
      const results = await api<Record<string, string>>('/api/registrations/supplemental-logging', {
        method: 'POST',
        body: JSON.stringify({ sourceConnectionId: sourceId, tables: needSupp.map(qualified) }),
      })
      setSuppResults((r) => ({ ...r, ...results }))
    })

  const register = () =>
    run(async () => {
      await api('/api/registrations', {
        method: 'POST',
        body: JSON.stringify({
          sourceConnectionId: sourceId,
          targetConnectionId: targetId,
          tables: picked,
        }),
      })
      onRegistered()
      onClose()
    })

  const toggle = (q: string) =>
    setPicked((p) => (p.includes(q) ? p.filter((x) => x !== q) : [...p, q]))

  const archivelogOk = dbChecks?.archivelog === 'ARCHIVELOG'
  const canNext = [
    sourceId != null,
    picked.length > 0,
    targetId != null,
    dbChecks != null && archivelogOk && needSupp.length === 0,
    true,
  ][step]

  const connCard = (c: DbConnection, selected: boolean, onClick: () => void) => (
    <div
      key={c.id}
      onClick={onClick}
      className={`cursor-pointer rounded-[10px] border p-3.5 ${
        selected ? 'border-primary bg-secondary' : 'border-border'
      }`}
    >
      <div className="text-[13.5px] font-semibold">{c.name}</div>
      <div className="mt-0.5 font-mono text-[11.5px] text-muted-foreground">
        {c.host}:{c.port}/{c.databaseName} · {c.username}
      </div>
    </div>
  )

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="flex max-h-[88vh] flex-col sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>새 CDC 등록</DialogTitle>
          <DialogDescription className="sr-only">CDC 테이블 등록 위저드</DialogDescription>
          <div className="mt-2 flex gap-1.5">
            {STEPS.map((s, i) => (
              <div key={s} className="flex-1">
                <div
                  className={`h-0.5 rounded ${i <= step ? 'bg-primary' : 'bg-border'}`}
                />
                <div
                  className={`mt-1 font-mono text-[10.5px] ${
                    i === step ? 'text-foreground' : 'text-muted-foreground'
                  }`}
                >
                  {i + 1}. {s}
                </div>
              </div>
            ))}
          </div>
        </DialogHeader>

        <div className="min-h-0 flex-1 overflow-y-auto py-1">
          {step === 0 && (
            <div className="grid gap-2.5">
              {connections.filter((c) => c.role === 'SOURCE').length === 0 && (
                <p className="text-sm text-muted-foreground">
                  SOURCE 역할 연결이 없습니다 — DB 연결 탭에서 먼저 등록하세요.
                </p>
              )}
              {connections
                .filter((c) => c.role === 'SOURCE')
                .map((c) => connCard(c, sourceId === c.id, () => setSourceId(c.id)))}
            </div>
          )}

          {step === 1 && (
            <div>
              <div className="mb-3 flex gap-2">
                <Input
                  value={pattern}
                  onChange={(e) => setPattern(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && pattern && void discover()}
                  placeholder="SCHEMA.* 또는 SCHEMA.TEST_* 또는 SCHEMA.TEST_TABLE_01"
                  className="font-mono"
                />
                <Button disabled={!pattern || busy} onClick={() => void discover()}>
                  조회
                </Button>
              </div>
              {candidates !== null && candidates.length === 0 && (
                <p className="text-sm text-muted-foreground">패턴에 걸리는 테이블이 없습니다.</p>
              )}
              <div className="grid gap-1.5">
                {(candidates ?? []).map((t) => {
                  const q = qualified(t)
                  const on = picked.includes(q)
                  return (
                    <div
                      key={q}
                      onClick={() => t.hasPk && toggle(q)}
                      className={`flex items-center gap-3 rounded-[9px] border px-3 py-2.5 ${
                        t.hasPk ? 'cursor-pointer' : 'opacity-50'
                      } ${on ? 'border-primary bg-secondary' : 'border-border'}`}
                    >
                      <div
                        className={`flex h-4 w-4 items-center justify-center rounded border text-[11px] font-bold ${
                          on
                            ? 'border-primary bg-primary text-primary-foreground'
                            : 'border-muted-foreground'
                        }`}
                      >
                        {on ? '✓' : ''}
                      </div>
                      <span className="font-mono text-[12.5px]">
                        {t.schema}.<b>{t.table}</b>
                      </span>
                      <div className="ml-auto flex items-center gap-2 font-mono text-[10.5px]">
                        {t.numRows != null && (
                          <span className="text-muted-foreground">
                            {t.numRows.toLocaleString()} rows
                          </span>
                        )}
                        {!t.hasPk && <span className="text-crit">PK 없음 — 등록 불가</span>}
                        {t.hasPk && !t.suppLogAll && (
                          <span className="text-warn">supp.log 미설정</span>
                        )}
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          )}

          {step === 2 && (
            <div className="grid gap-2.5">
              {connections.filter((c) => c.role === 'TARGET').length === 0 && (
                <p className="text-sm text-muted-foreground">
                  TARGET 역할 연결이 없습니다 — DB 연결 탭에서 먼저 등록하세요.
                </p>
              )}
              {connections
                .filter((c) => c.role === 'TARGET')
                .map((c) => connCard(c, targetId === c.id, () => setTargetId(c.id)))}
            </div>
          )}

          {step === 3 && (
            <div className="grid gap-2.5">
              {dbChecks === null ? (
                <Button disabled={busy} onClick={() => void loadChecks()}>
                  DB 레벨 점검 실행
                </Button>
              ) : (
                <>
                  <CheckRow
                    ok={archivelogOk}
                    label="ARCHIVELOG 모드"
                    detail={dbChecks.archivelog}
                  />
                  <CheckRow
                    ok={dbChecks.db_supplemental_log_min === 'YES'}
                    warnOnly
                    label="DB 최소 supplemental logging"
                    detail={dbChecks.db_supplemental_log_min}
                  />
                </>
              )}

              <div className="mt-1 text-[13px] font-semibold">
                테이블 supplemental logging (ALL) COLUMNS
              </div>
              {pickedInfos.map((t) => {
                const q = qualified(t)
                const applied = t.suppLogAll || suppResults?.[q] === 'OK'
                const failMsg =
                  suppResults?.[q] && suppResults[q] !== 'OK' ? suppResults[q] : null
                return (
                  <CheckRow
                    key={q}
                    ok={applied}
                    label={q}
                    detail={
                      applied
                        ? '설정됨'
                        : failMsg ?? '설정이 필요합니다 — 아래에서 적용을 승인하세요'
                    }
                  />
                )
              })}

              {needSupp.length > 0 && (
                <div className="rounded-[10px] border border-warn bg-warn/10 p-3 text-xs leading-relaxed">
                  <p className="mb-2">
                    {needSupp.length}개 테이블에 다음 DDL을 실행해야 CDC 등록이 가능합니다:
                  </p>
                  <pre className="overflow-x-auto rounded bg-background p-2 font-mono text-[11px]">
                    {needSupp
                      .map(
                        (t) =>
                          `ALTER TABLE ${qualified(t)} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;`,
                      )
                      .join('\n')}
                  </pre>
                  <p className="mt-2 font-semibold">적용하겠습니까?</p>
                  <Button size="sm" className="mt-1.5" disabled={busy} onClick={() => void applySupp()}>
                    YES — 소스에 DDL 적용
                  </Button>
                  <p className="mt-1.5 text-muted-foreground">
                    권한이 없으면 Oracle 에러 메시지가 표시됩니다 — DBA에게 위 DDL 실행을 요청하세요.
                  </p>
                </div>
              )}
            </div>
          )}

          {step === 4 && (
            <div className="grid gap-3 text-sm">
              <div className="rounded-[10px] border border-border bg-secondary p-3 font-mono text-[12px] leading-relaxed">
                <div>tables: {picked.join(', ')}</div>
                <div>
                  source: {connections.find((c) => c.id === sourceId)?.name} → target:{' '}
                  {connections.find((c) => c.id === targetId)?.name}
                </div>
                <div className="mt-2 text-muted-foreground">
                  배포되는 커넥터: dz-source (LogMiner 캡처) · dz-jdbc-sink (PK upsert apply) ·
                  dz-iceberg-sink (changelog 병행 적재)
                </div>
              </div>
              <p className="text-xs leading-relaxed text-muted-foreground">
                초기 스냅샷(initial) 후 실시간 CDC로 전환됩니다. changelog 테이블
                (changelog.&lt;스키마&gt;_&lt;테이블&gt;)은 배포 시 자동 사전 생성됩니다 —
                별도 설정이 필요 없습니다.
              </p>
            </div>
          )}

          {error && <p className="mt-3 text-sm text-destructive">{error}</p>}
        </div>

        <DialogFooter>
          <Button variant="ghost" onClick={onClose}>
            취소
          </Button>
          {step > 0 && (
            <Button variant="outline" onClick={() => setStep(step - 1)}>
              이전
            </Button>
          )}
          {step < 4 ? (
            <Button
              disabled={!canNext || busy}
              onClick={() => {
                if (step === 2) setDbChecks(null)
                setStep(step + 1)
              }}
            >
              다음
            </Button>
          ) : (
            <Button disabled={busy} onClick={() => void register()}>
              배포
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function CheckRow({
  ok,
  label,
  detail,
  warnOnly,
}: {
  ok: boolean
  label: string
  detail: string
  warnOnly?: boolean
}) {
  return (
    <div className="flex items-start gap-3 rounded-[10px] border border-border bg-secondary px-3.5 py-3">
      <span className={`font-mono text-sm ${ok ? 'text-ok' : warnOnly ? 'text-warn' : 'text-crit'}`}>
        {ok ? '✓' : warnOnly ? '!' : '✕'}
      </span>
      <div className="min-w-0 flex-1">
        <div className="text-[13px] font-semibold">{label}</div>
        <div className="mt-0.5 break-words font-mono text-[11.5px] leading-relaxed text-muted-foreground">
          {detail}
        </div>
      </div>
      {!ok && !warnOnly && <Badge variant="destructive">필수</Badge>}
    </div>
  )
}

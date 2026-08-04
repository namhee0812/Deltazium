/**
 * 파일명 : RegistrationWizard.tsx
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : 6단계 CDC 등록 위저드 — 소스/타깃 선택, 딕셔너리 조회, 컬럼 매핑, 사전 점검, 스냅샷 모드, 배포.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
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

/* CDC 등록 위저드 (6단계) — backend 실연동
   정책: PK 없는 테이블 선택 불가(A안) · supp.log는 승인 후 적용 · 권한은 DBA 안내 ·
   컬럼 매핑은 ${소스컬럼} 형식만 (함수·치환은 추후), PK는 동일명·활성 필수 */

interface SourceTableInfo {
  schema: string
  table: string
  hasPk: boolean
  suppLogAll: boolean
  numRows: number | null
}

interface TableColumn {
  name: string
  dataType: string
  pk: boolean
}

interface MappingRow {
  targetColumn: string
  dataType: string
  enabled: boolean
  sourceExpr: string
}

interface TableMapping {
  targetSchema: string
  targetTable: string
  sourceColumns: TableColumn[] | null
  rows: MappingRow[] | null
  loadError: string | null
}

const STEPS = ['소스', '테이블', '타깃', '컬럼 매핑', '사전 점검', '검토·배포'] as const

const EXPR_RE = /^\$\{[A-Za-z][A-Za-z0-9_#$]*\}$/

const qualified = (t: SourceTableInfo) => `${t.schema}.${t.table}`
const exprSource = (expr: string) =>
  EXPR_RE.test(expr.trim()) ? expr.trim().slice(2, -1).toUpperCase() : null

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

  const [mappings, setMappings] = useState<Record<string, TableMapping>>({})

  const [dbChecks, setDbChecks] = useState<Record<string, string> | null>(null)
  const [privChecks, setPrivChecks] = useState<Record<string, boolean> | null>(null)
  const [suppResults, setSuppResults] = useState<Record<string, string> | null>(null)

  const [snapshotMode, setSnapshotMode] = useState<'INITIAL' | 'NO_DATA'>('INITIAL')
  const [existingCount, setExistingCount] = useState(0)

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
    setMappings({})
    setDbChecks(null)
    setPrivChecks(null)
    setSuppResults(null)
    setSnapshotMode('INITIAL')
    setError(null)
    api<DbConnection[]>('/api/connections').then(setConnections).catch(() => setConnections([]))
    api<unknown[]>('/api/registrations')
      .then((r) => setExistingCount(r.length))
      .catch(() => setExistingCount(0))
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

  const toggle = (q: string) =>
    setPicked((p) => (p.includes(q) ? p.filter((x) => x !== q) : [...p, q]))

  /* ── 컬럼 매핑 단계 ── */

  const ensureMappingEntries = () => {
    setMappings((m) => {
      const next = { ...m }
      for (const q of picked) {
        if (!next[q]) {
          const [schema, table] = q.split('.')
          next[q] = {
            targetSchema: schema,
            targetTable: table,
            sourceColumns: null,
            rows: null,
            loadError: null,
          }
        }
      }
      return next
    })
  }

  const patchMapping = (q: string, patch: Partial<TableMapping>) =>
    setMappings((m) => ({ ...m, [q]: { ...m[q], ...patch } }))

  const loadColumns = (q: string) =>
    run(async () => {
      const entry = mappings[q]
      try {
        const sourceColumns = await api<TableColumn[]>(
          `/api/registrations/columns/${sourceId}?table=${encodeURIComponent(q)}`,
        )
        const targetColumns = await api<TableColumn[]>(
          `/api/registrations/columns/${targetId}?table=${encodeURIComponent(
            `${entry.targetSchema}.${entry.targetTable}`,
          )}`,
        )
        if (targetColumns.length === 0) {
          patchMapping(q, {
            loadError: `타깃에 ${entry.targetSchema}.${entry.targetTable} 테이블이 없거나 컬럼 조회 권한이 없습니다`,
            rows: null,
          })
          return
        }
        const sourceNames = new Set(sourceColumns.map((c) => c.name.toUpperCase()))
        // 동일 이름은 자동 매핑, 나머지는 빈 칸(사용자 입력 대기)
        const rows: MappingRow[] = targetColumns.map((c) => ({
          targetColumn: c.name,
          dataType: c.dataType,
          enabled: sourceNames.has(c.name.toUpperCase()),
          sourceExpr: sourceNames.has(c.name.toUpperCase()) ? `\${${c.name}}` : '',
        }))
        patchMapping(q, { sourceColumns, rows, loadError: null })
      } catch (e) {
        patchMapping(q, { loadError: (e as Error).message, rows: null })
      }
    })

  const patchRow = (q: string, idx: number, patch: Partial<MappingRow>) =>
    setMappings((m) => {
      const rows = [...(m[q].rows ?? [])]
      rows[idx] = { ...rows[idx], ...patch }
      return { ...m, [q]: { ...m[q], rows } }
    })

  /** 매핑 검증 — 오류 목록 (비면 통과). 다음 버튼 게이트 + 인라인 표시 공용 */
  const mappingErrors = (q: string): string[] => {
    const entry = mappings[q]
    if (!entry) return ['매핑 미로드']
    if (!entry.targetSchema.trim() || !entry.targetTable.trim())
      return ['타깃 스키마·테이블명을 입력하세요']
    if (!entry.rows) return ['타깃 컬럼을 불러오세요']
    const errors: string[] = []
    const sourceNames = new Set((entry.sourceColumns ?? []).map((c) => c.name.toUpperCase()))
    const pkNames = (entry.sourceColumns ?? []).filter((c) => c.pk).map((c) => c.name.toUpperCase())
    const enabledIdentity = new Set<string>()
    for (const row of entry.rows) {
      if (!row.enabled) continue
      const src = exprSource(row.sourceExpr)
      if (!src) {
        errors.push(`${row.targetColumn}: 변환식 구문 오류 — \${소스컬럼} 형식만 허용 ("${row.sourceExpr}")`)
        continue
      }
      if (!sourceNames.has(src)) {
        errors.push(`${row.targetColumn}: 소스에 없는 컬럼 참조 (${row.sourceExpr})`)
        continue
      }
      if (src === row.targetColumn.toUpperCase()) enabledIdentity.add(src)
    }
    for (const pk of pkNames) {
      if (!enabledIdentity.has(pk))
        errors.push(`PK 컬럼 ${pk}는 동일명으로 매핑·활성화돼야 합니다 (upsert key)`)
    }
    return errors
  }

  const allMappingsValid = picked.every((q) => mappingErrors(q).length === 0)

  /* ── 사전 점검 단계 ── */

  const loadChecks = () =>
    run(async () => {
      setDbChecks(await api<Record<string, string>>(`/api/registrations/db-checks/${sourceId}`))
      setPrivChecks(
        await api<Record<string, boolean>>(`/api/registrations/privilege-checks/${sourceId}`),
      )
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
          snapshotMode,
          tables: picked.map((q) => {
            const m = mappings[q]
            return {
              source: q,
              targetSchema: m.targetSchema,
              targetTable: m.targetTable,
              columns: (m.rows ?? []).map((r) => ({
                targetColumn: r.targetColumn,
                sourceExpr: r.sourceExpr,
                enabled: r.enabled,
              })),
            }
          }),
        }),
      })
      onRegistered()
      onClose()
    })

  const archivelogOk = dbChecks?.archivelog === 'ARCHIVELOG'
  const missingPrivs = Object.entries(privChecks ?? {})
    .filter(([, ok]) => !ok)
    .map(([name]) => name)
  const privsOk = privChecks !== null && missingPrivs.length === 0
  const canNext = [
    sourceId != null,
    picked.length > 0,
    targetId != null,
    allMappingsValid,
    dbChecks != null && archivelogOk && privsOk && needSupp.length === 0,
    true,
  ][step]

  const sourceUser = connections.find((c) => c.id === sourceId)?.username ?? '<캡처계정>'
  const grantScript = missingPrivs
    .map((p) =>
      p.startsWith('EXECUTE ON ')
        ? `GRANT EXECUTE ON SYS.${p.slice('EXECUTE ON '.length)} TO ${sourceUser};`
        : `GRANT ${p} TO ${sourceUser};`,
    )
    .join('\n')

  const renameCount = picked.reduce((n, q) => {
    const rows = mappings[q]?.rows ?? []
    return (
      n +
      rows.filter((r) => {
        const src = exprSource(r.sourceExpr)
        return r.enabled && src && src !== r.targetColumn.toUpperCase()
      }).length
    )
  }, 0)

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
      <DialogContent className="flex max-h-[90vh] flex-col sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>새 CDC 등록</DialogTitle>
          <DialogDescription className="sr-only">CDC 테이블 등록 위저드</DialogDescription>
          <div className="mt-2 flex gap-1.5">
            {STEPS.map((s, i) => (
              <div key={s} className="flex-1">
                <div className={`h-0.5 rounded ${i <= step ? 'bg-primary' : 'bg-border'}`} />
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
            <div className="grid gap-4">
              <p className="text-xs leading-relaxed text-muted-foreground">
                타깃 테이블·컬럼 매핑을 확인하세요. 소스와 이름이 같은 컬럼은 자동으로
                채워집니다. 변환식은 <code className="font-mono">{'${소스컬럼}'}</code> 형식만
                지원합니다 (함수·치환은 추후). 체크 해제한 컬럼은 CDC 적재에서 제외됩니다.
              </p>
              {picked.map((q) => {
                const entry = mappings[q]
                if (!entry) return null
                const errors = mappingErrors(q)
                return (
                  <div key={q} className="rounded-[10px] border border-border p-3.5">
                    <div className="mb-2 flex flex-wrap items-center gap-2">
                      <span className="font-mono text-[13px] font-semibold">{q}</span>
                      <span className="text-muted-foreground">→</span>
                      <Input
                        value={entry.targetSchema}
                        onChange={(e) => patchMapping(q, { targetSchema: e.target.value, rows: null })}
                        className="w-32 font-mono"
                        placeholder="타깃 스키마"
                      />
                      <span className="text-muted-foreground">.</span>
                      <Input
                        value={entry.targetTable}
                        onChange={(e) => patchMapping(q, { targetTable: e.target.value, rows: null })}
                        className="w-40 font-mono"
                        placeholder="타깃 테이블"
                      />
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => void loadColumns(q)}
                      >
                        타깃 컬럼 불러오기
                      </Button>
                    </div>

                    {entry.loadError && (
                      <p className="text-xs text-destructive">{entry.loadError}</p>
                    )}

                    {entry.rows && (
                      <table className="w-full text-[12px]">
                        <thead>
                          <tr className="text-left font-mono text-[10px] text-muted-foreground">
                            <th className="w-8 py-1">적재</th>
                            <th className="py-1">타깃 컬럼</th>
                            <th className="py-1">타입</th>
                            <th className="py-1">변환식 (소스)</th>
                          </tr>
                        </thead>
                        <tbody>
                          {entry.rows.map((row, idx) => {
                            const src = exprSource(row.sourceExpr)
                            const bad =
                              row.enabled &&
                              (!src ||
                                !(entry.sourceColumns ?? []).some(
                                  (c) => c.name.toUpperCase() === src,
                                ))
                            const rename =
                              row.enabled && src && src !== row.targetColumn.toUpperCase()
                            return (
                              <tr key={row.targetColumn} className="border-t border-border/60">
                                <td className="py-1.5">
                                  <input
                                    type="checkbox"
                                    checked={row.enabled}
                                    onChange={(e) =>
                                      patchRow(q, idx, { enabled: e.target.checked })
                                    }
                                  />
                                </td>
                                <td className="py-1.5 font-mono">{row.targetColumn}</td>
                                <td className="py-1.5 font-mono text-[10.5px] text-muted-foreground">
                                  {row.dataType}
                                </td>
                                <td className="py-1.5">
                                  <div className="flex items-center gap-2">
                                    <Input
                                      value={row.sourceExpr}
                                      disabled={!row.enabled}
                                      onChange={(e) =>
                                        patchRow(q, idx, { sourceExpr: e.target.value })
                                      }
                                      placeholder="${SRC_COL}"
                                      className={`h-7 w-44 font-mono text-[11.5px] ${
                                        bad ? 'border-crit' : ''
                                      }`}
                                    />
                                    {rename && (
                                      <Badge variant="outline" className="text-warn">
                                        리네임 — 적재 미지원(저장만)
                                      </Badge>
                                    )}
                                  </div>
                                </td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    )}

                    {errors.length > 0 && entry.rows && (
                      <ul className="mt-2 list-inside list-disc text-xs text-destructive">
                        {errors.map((e) => (
                          <li key={e}>{e}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                )
              })}
            </div>
          )}

          {step === 4 && (
            <div className="grid gap-2.5">
              {dbChecks === null ? (
                <Button disabled={busy} onClick={() => void loadChecks()}>
                  DB 레벨 점검 실행
                </Button>
              ) : (
                <>
                  <CheckRow ok={archivelogOk} label="ARCHIVELOG 모드" detail={dbChecks.archivelog} />
                  <CheckRow
                    ok={dbChecks.db_supplemental_log_min === 'YES'}
                    warnOnly
                    label="DB 최소 supplemental logging"
                    detail={dbChecks.db_supplemental_log_min}
                  />
                </>
              )}

              {privChecks !== null && (
                <>
                  <div className="mt-1 text-[13px] font-semibold">캡처 계정 권한 (최소 8개)</div>
                  {Object.entries(privChecks).map(([name, ok]) => (
                    <CheckRow key={name} ok={ok} label={name} detail={ok ? '보유' : '누락'} />
                  ))}
                  {missingPrivs.length > 0 && (
                    <div className="rounded-[10px] border border-crit bg-crit/10 p-3 text-xs leading-relaxed">
                      <p className="mb-2">
                        누락 권한 {missingPrivs.length}개 — 계정 스스로 부여할 수 없어
                        <b> DBA 계정으로</b> 아래 GRANT를 실행해야 합니다. 실행 후 [재점검]을
                        눌러 확인하세요.
                      </p>
                      <pre className="overflow-x-auto rounded bg-background p-2 font-mono text-[11px]">
                        {grantScript}
                      </pre>
                    </div>
                  )}
                </>
              )}

              <div className="mt-1 text-[13px] font-semibold">
                테이블 supplemental logging (ALL) COLUMNS
              </div>
              {pickedInfos.map((t) => {
                const q = qualified(t)
                const applied = t.suppLogAll || suppResults?.[q] === 'OK'
                const failMsg = suppResults?.[q] && suppResults[q] !== 'OK' ? suppResults[q] : null
                return (
                  <CheckRow
                    key={q}
                    ok={applied}
                    label={q}
                    detail={
                      applied ? '설정됨' : failMsg ?? '설정이 필요합니다 — 아래에서 적용을 승인하세요'
                    }
                  />
                )
              })}

              {dbChecks !== null && (
                <Button variant="outline" size="sm" disabled={busy} onClick={() => void loadChecks()}>
                  재점검
                </Button>
              )}

              {needSupp.length > 0 && (
                <div className="rounded-[10px] border border-warn bg-warn/10 p-3 text-xs leading-relaxed">
                  <p className="mb-2">
                    {needSupp.length}개 테이블에 다음 DDL을 실행해야 CDC 등록이 가능합니다:
                  </p>
                  <pre className="overflow-x-auto rounded bg-background p-2 font-mono text-[11px]">
                    {needSupp
                      .map(
                        (t) => `ALTER TABLE ${qualified(t)} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;`,
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

          {step === 5 && (
            <div className="grid gap-3 text-sm">
              <div className="rounded-[10px] border border-border p-3.5">
                <div className="mb-2 text-[13px] font-semibold">시작 방식 (초기 스냅샷)</div>
                {existingCount > 0 ? (
                  <p className="text-xs leading-relaxed text-muted-foreground">
                    이미 CDC가 기동 중이라 시작 방식을 선택할 수 없습니다 — snapshot.mode는
                    커넥터 전역 설정이라 첫 등록에서만 적용됩니다. 추가 테이블의 초기적재는
                    테이블별 스냅샷(incremental snapshot) 기능으로 지원 예정입니다.
                  </p>
                ) : (
                  <div className="grid gap-2">
                    <label className="flex cursor-pointer items-start gap-2">
                      <input
                        type="radio"
                        className="mt-1"
                        checked={snapshotMode === 'INITIAL'}
                        onChange={() => setSnapshotMode('INITIAL')}
                      />
                      <span>
                        <b className="text-[13px]">초기적재 포함 (initial)</b>
                        <span className="block text-xs text-muted-foreground">
                          현재 데이터 전체를 스냅샷으로 적재한 뒤 실시간 CDC로 전환합니다.
                          changelog에 초기 상태(op='r')까지 남아 SCN 0부터 전체 복원이 가능합니다.
                        </span>
                      </span>
                    </label>
                    <label className="flex cursor-pointer items-start gap-2">
                      <input
                        type="radio"
                        className="mt-1"
                        checked={snapshotMode === 'NO_DATA'}
                        onChange={() => setSnapshotMode('NO_DATA')}
                      />
                      <span>
                        <b className="text-[13px]">현재 시점부터 변경만 (no_data)</b>
                        <span className="block text-xs text-muted-foreground">
                          초기적재 없이 지금부터의 변경만 캡처합니다. 초기 데이터는 별도
                          일괄적재로 맞추는 경우에 사용하세요.
                        </span>
                        <span className="block text-xs text-warn">
                          ⚠ changelog에 초기 상태가 없으므로 SCN 재발행 복구 범위가 CDC 시작
                          시점 이후로 제한됩니다.
                        </span>
                      </span>
                    </label>
                  </div>
                )}
              </div>

              <div className="rounded-[10px] border border-border bg-secondary p-3 font-mono text-[12px] leading-relaxed">
                {picked.map((q) => {
                  const m = mappings[q]
                  const enabled = (m.rows ?? []).filter((r) => r.enabled).length
                  return (
                    <div key={q}>
                      {q} → {m.targetSchema}.{m.targetTable} ({enabled}/{m.rows?.length ?? 0} 컬럼)
                    </div>
                  )
                })}
                <div className="mt-2 text-muted-foreground">
                  source: {connections.find((c) => c.id === sourceId)?.name} → target:{' '}
                  {connections.find((c) => c.id === targetId)?.name}
                </div>
                <div className="text-muted-foreground">
                  커넥터: dz-source · 테이블별 dz-jdbc-sink-* · dz-iceberg-sink(changelog)
                </div>
              </div>
              {renameCount > 0 && (
                <p className="text-xs leading-relaxed text-warn">
                  리네임 매핑 {renameCount}건은 저장만 되고 적재에는 아직 반영되지 않습니다
                  (스톡 sink 한계 — 지원 방안 검토 중). 해당 컬럼은 적재에서 제외됩니다.
                </p>
              )}
              <p className="text-xs leading-relaxed text-muted-foreground">
                초기 스냅샷(initial) 후 실시간 CDC로 전환됩니다. changelog 테이블은 배포 시 자동
                사전 생성되며 컬럼 매핑과 무관하게 소스 원본을 보존합니다(복구 원본).
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
          {step < 5 ? (
            <Button
              disabled={!canNext || busy}
              onClick={() => {
                if (step === 2) ensureMappingEntries()
                if (step === 3) setDbChecks(null)
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

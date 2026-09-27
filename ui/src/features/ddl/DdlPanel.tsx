/**
 * 파일명 : DdlPanel.tsx
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : DDL 이력 타임라인 — 수집 이벤트 조회, 승인/거부, 비전파성 DDL 자동 무시 표시.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 27.       | 최남희  | 하드코딩 hex를 CSS 변수/토큰 클래스로 교체 — 라이트 테마 대응
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: origin(SCHEMA_TOPIC|FINGERPRINT) 배지
 * |                          | 추가 — PostgreSQL 등 schema change topic이 없는 소스는 스키마
 * |                          | 지문 비교로 감지된다(architecture.md 7절)
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 테이블별 DDL 반영 정책(architecture.md 7절 개정): state에
 * |                          | AUTO_APPLIED·SKIPPED 추가, DETECTED 카드 버튼을 [적용 후 재개]
 * |                          | [건너뛰고 재개](신규 skip API) [정지 유지] 3개로, 카드에 정책
 * |                          | 배지(자동/확인 후 — /api/registrations 조회)를 추가했다
 * --------------------------------------------------
 */
import { useCallback, useEffect, useState } from 'react'
import { Button } from '@/components/ui/button'
import { api } from '@/lib/api'

/* DDL 타임라인 (ui-reference v3) — 실데이터: backend가 schema change topic을 상시 소비해
   적재한 이벤트. 적용 후 재개=타깃 DDL 적용, 건너뛰고 재개=매핑만 조정, 정지 유지=거부(7절) */

interface DdlEvent {
  id: number
  kafkaOffset: number | null
  eventTsMs: number
  scn: string | null
  schemaName: string | null
  tableName: string | null
  ddlText: string
  state: 'SNAPSHOT' | 'DETECTED' | 'APPROVED' | 'REJECTED' | 'IGNORED' | 'AUTO_APPLIED' | 'SKIPPED'
  note: string | null
  decidedAt: string | null
  origin: 'SCHEMA_TOPIC' | 'FINGERPRINT'
}

/** 테이블 정책 배지 표시용 — /api/registrations(RegisteredTableView)의 일부만 쓴다. */
interface RegisteredTableForPolicy {
  schemaName: string
  tableName: string
  ddlPolicy: string
}

const originLabel: Record<DdlEvent['origin'], string> = {
  SCHEMA_TOPIC: 'schema change topic',
  FINGERPRINT: '스키마 지문 비교',
}

const stateColor: Record<DdlEvent['state'], string> = {
  DETECTED: 'text-warn bg-warn/10',
  APPROVED: 'text-ok bg-ok/10',
  REJECTED: 'text-crit bg-crit/10',
  SNAPSHOT: 'text-muted-foreground bg-secondary',
  IGNORED: 'text-muted-foreground bg-secondary',
  AUTO_APPLIED: 'text-ok bg-ok/10',
  SKIPPED: 'text-muted-foreground bg-secondary',
}

const dotColor: Record<DdlEvent['state'], string> = {
  DETECTED: 'var(--warn)',
  APPROVED: 'var(--ok)',
  REJECTED: 'var(--crit)',
  SNAPSHOT: 'var(--chart-dim)',
  IGNORED: 'var(--chart-dim)',
  AUTO_APPLIED: 'var(--ok)',
  SKIPPED: 'var(--chart-dim)',
}

const stateLabel: Record<DdlEvent['state'], string> = {
  DETECTED: '승인 대기',
  APPROVED: 'approved',
  REJECTED: 'rejected',
  SNAPSHOT: 'snapshot',
  IGNORED: '무시됨',
  AUTO_APPLIED: '자동 적용됨',
  SKIPPED: '건너뜀',
}

const FILTERS = [
  'all', 'DETECTED', 'APPROVED', 'REJECTED', 'AUTO_APPLIED', 'SKIPPED', 'SNAPSHOT', 'IGNORED',
] as const

export function DdlPanel() {
  const [events, setEvents] = useState<DdlEvent[] | null>(null)
  const [tables, setTables] = useState<RegisteredTableForPolicy[]>([])
  const [filter, setFilter] = useState<(typeof FILTERS)[number]>('all')
  const [open, setOpen] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const load = useCallback(() => {
    api<DdlEvent[]>('/api/ddl-events')
      .then((data) => {
        setEvents(data)
        setError(null)
      })
      .catch((e: Error) => setError(e.message))
    api<RegisteredTableForPolicy[]>('/api/registrations').then(setTables).catch(() => {})
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(load, 10000)
    return () => clearInterval(id)
  }, [load])

  /** 이벤트가 가리키는 테이블의 DDL 반영 정책 — 등록 정보가 아직 없으면(미등록 테이블의
   * DDL 등) 배지를 생략한다. */
  const policyOf = (e: DdlEvent): string | null =>
    tables.find(
      (t) =>
        e.schemaName?.toUpperCase() === t.schemaName.toUpperCase()
        && e.tableName?.toUpperCase() === t.tableName.toUpperCase(),
    )?.ddlPolicy ?? null

  const decide = async (id: number, action: 'approve' | 'reject' | 'skip') => {
    setBusy(true)
    setError(null)
    try {
      await api(`/api/ddl-events/${id}/${action}`, { method: 'POST' })
      load()
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setBusy(false)
    }
  }

  const visible = (events ?? []).filter((e) => filter === 'all' || e.state === filter)
  const fmt = (ms: number) => new Date(ms).toLocaleString('sv-SE').replace('T', ' ')

  return (
    <div className="h-full overflow-y-auto p-5">
      <div className="mb-4 flex items-center gap-1.5">
        {FILTERS.map((f) => (
          <Button
            key={f}
            size="sm"
            variant={filter === f ? 'default' : 'ghost'}
            onClick={() => setFilter(f)}
          >
            {f === 'all' ? '전체' : stateLabel[f]}
          </Button>
        ))}
        <span className="ml-auto font-mono text-[11px] text-muted-foreground">
          {visible.length} events
        </span>
      </div>

      {error && <p className="mb-3 text-sm text-destructive">{error}</p>}
      {events !== null && events.length === 0 && !error && (
        <p className="text-sm text-muted-foreground">
          수집된 DDL 이벤트가 없습니다 — 소스에서 DDL이 발생하면 여기에 쌓입니다.
        </p>
      )}

      <div className="relative pl-6">
        {visible.length > 0 && (
          <div className="absolute bottom-1 left-2 top-1 w-0.5 bg-border" />
        )}
        <div className="grid gap-3">
          {visible.map((e) => {
            const isOpen = open === e.id
            return (
              <div key={e.id} className="relative">
                <span
                  className="absolute -left-6 top-3.5 h-2.5 w-2.5 rounded-full border-2 border-background"
                  style={{ background: dotColor[e.state] }}
                />
                <div
                  onClick={() => setOpen(isOpen ? null : e.id)}
                  className={`cursor-pointer rounded-xl border bg-card px-4 py-3 ${
                    isOpen ? 'border-primary' : 'border-border'
                  }`}
                >
                  <div className="flex flex-wrap items-center gap-3">
                    <span className="font-mono text-[11px] text-muted-foreground">
                      {fmt(e.eventTsMs)}
                    </span>
                    <span className="font-mono text-[12.5px] font-semibold">
                      {e.schemaName && e.tableName
                        ? `${e.schemaName}.${e.tableName}`
                        : '(테이블 미상)'}
                    </span>
                    <span
                      className={`ml-auto rounded px-2 py-0.5 font-mono text-[10.5px] ${stateColor[e.state]}`}
                    >
                      {stateLabel[e.state]}
                    </span>
                  </div>
                  {isOpen && (
                    <div className="mt-3">
                      <pre className="overflow-x-auto rounded-lg border border-border bg-background p-3 font-mono text-[11.5px] leading-relaxed text-foreground/90">
                        {e.ddlText}
                      </pre>
                      <div className="mt-2 flex flex-wrap items-center gap-3 font-mono text-[11px] text-muted-foreground">
                        <span className="rounded bg-secondary px-1.5 py-0.5">{originLabel[e.origin]}</span>
                        {policyOf(e) && (
                          <span className="rounded bg-secondary px-1.5 py-0.5">
                            정책: {policyOf(e) === 'AUTO' ? '자동' : '확인 후'}
                          </span>
                        )}
                        {e.scn && <span>SCN {e.scn}</span>}
                        {e.kafkaOffset != null && <span>offset {e.kafkaOffset}</span>}
                        {e.note && <span className="text-foreground">{e.note}</span>}
                      </div>
                      {e.state === 'DETECTED' && (
                        <div className="mt-3 flex flex-wrap gap-2">
                          <Button
                            size="sm"
                            disabled={busy}
                            onClick={(ev) => {
                              ev.stopPropagation()
                              void decide(e.id, 'approve')
                            }}
                          >
                            적용 후 재개
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={busy}
                            onClick={(ev) => {
                              ev.stopPropagation()
                              void decide(e.id, 'skip')
                            }}
                          >
                            건너뛰고 재개
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={busy}
                            onClick={(ev) => {
                              ev.stopPropagation()
                              void decide(e.id, 'reject')
                            }}
                          >
                            정지 유지
                          </Button>
                        </div>
                      )}
                      {e.state === 'REJECTED' && (
                        <p className="mt-3 text-xs text-crit">
                          apply 정지 중 — changelog는 계속 축적됩니다. 재개(캐치업)는 복구
                          재발행 경로로, 마일스톤 5에서 구현 예정.
                        </p>
                      )}
                    </div>
                  )}
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

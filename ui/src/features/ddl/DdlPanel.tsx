import { useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { mockDdlEvents } from '@/lib/mock'
import type { DdlEvent } from '@/lib/mock'

/* DDL 타임라인 (ui-reference v3) — schema change topic 구독 API 전까지 mock 데이터.
   승인/거부 흐름은 architecture.md 7절: 승인=타깃 DDL 적용·재개, 거부=jdbc-sink에서 토픽 제외 */

const stateColor: Record<DdlEvent['state'], string> = {
  applied: 'text-ok bg-ok/10',
  pending: 'text-warn bg-warn/10',
  blocked: 'text-crit bg-crit/10',
  skipped: 'text-muted-foreground bg-secondary',
}

const dotColor: Record<DdlEvent['state'], string> = {
  applied: '#56D89C',
  pending: '#F5B453',
  blocked: '#F0647A',
  skipped: '#8A97B4',
}

const FILTERS = ['all', 'applied', 'pending', 'blocked', 'skipped'] as const

export function DdlPanel() {
  const [events, setEvents] = useState<DdlEvent[]>(mockDdlEvents)
  const [filter, setFilter] = useState<(typeof FILTERS)[number]>('all')
  const [open, setOpen] = useState<number | null>(4)

  const visible = events.filter((e) => filter === 'all' || e.state === filter)

  const decide = (id: number, state: DdlEvent['state'], note: string) =>
    setEvents((es) => es.map((e) => (e.id === id ? { ...e, state, note } : e)))

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
            {f === 'all' ? '전체' : f}
          </Button>
        ))}
        <Badge variant="outline" className="ml-2 text-warn">mock 데이터</Badge>
        <span className="ml-auto font-mono text-[11px] text-muted-foreground">
          {visible.length} events · 최근 7일
        </span>
      </div>

      <div className="relative pl-6">
        <div className="absolute bottom-1 left-2 top-1 w-0.5 bg-border" />
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
                    <span className="font-mono text-[11px] text-muted-foreground">{e.ts}</span>
                    <span className="font-mono text-[12.5px] font-semibold">{e.table}</span>
                    <span className="rounded bg-secondary px-2 py-0.5 font-mono text-[11px]">
                      {e.op}
                    </span>
                    <span
                      className={`ml-auto rounded px-2 py-0.5 font-mono text-[10.5px] ${stateColor[e.state]}`}
                    >
                      {e.state}
                    </span>
                  </div>
                  {isOpen && (
                    <div className="mt-3">
                      <pre className="overflow-x-auto rounded-lg border border-border bg-background p-3 font-mono text-[11.5px] leading-relaxed text-[#B9C7E4]">
                        {e.sql}
                      </pre>
                      <div className="mt-2 flex flex-wrap gap-3 font-mono text-[11px] text-muted-foreground">
                        <span>SCN {e.scn}</span>
                        <span className="text-foreground">{e.note}</span>
                      </div>
                      {e.state === 'pending' && (
                        <div className="mt-3 flex gap-2">
                          <Button
                            size="sm"
                            onClick={(ev) => {
                              ev.stopPropagation()
                              decide(e.id, 'applied', '타깃 Oracle에 DDL 적용 — jdbc-sink 재개 (mock)')
                            }}
                          >
                            승인 — 타깃에 적용
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={(ev) => {
                              ev.stopPropagation()
                              decide(e.id, 'blocked', '거부됨 — jdbc-sink에서 해당 테이블 토픽 제외 (mock)')
                            }}
                          >
                            거부 — apply 정지
                          </Button>
                        </div>
                      )}
                      {e.state === 'blocked' && (
                        <p className="mt-3 text-xs text-crit">
                          apply 정지 중 — changelog는 계속 축적됩니다. 스키마 정리 후 [재개]하면
                          재발행 경로로 캐치업합니다.
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

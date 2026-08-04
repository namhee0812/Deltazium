/**
 * 파일명 : EventsPanel.tsx
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 테이블별 운영 이벤트 타임라인 — 커넥터 장애 전이 trace 포함.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
import { useCallback, useEffect, useState } from 'react'
import { api } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'

/* 테이블별 운영 이벤트 타임라인 — 등록/정지/삭제·DDL 승인·복구·커넥터 장애 전이(trace 포함).
   "무슨 일이 언제 있었나"를 한곳에서 보는 화면. 원시 로그는 서버에서 (deploy/README 참고) */

interface TableEvent {
  id: number
  occurredAt: string
  schemaName: string
  tableName: string
  eventType: string
  severity: 'INFO' | 'WARN' | 'ERROR'
  message: string
  detail: string | null
}

const sevColor: Record<TableEvent['severity'], string> = {
  INFO: 'text-ok bg-ok/10',
  WARN: 'text-warn bg-warn/10',
  ERROR: 'text-crit bg-crit/10',
}

const dotColor: Record<TableEvent['severity'], string> = {
  INFO: '#56D89C',
  WARN: '#F5B453',
  ERROR: '#F0647A',
}

export function EventsPanel() {
  const [events, setEvents] = useState<TableEvent[] | null>(null)
  const [filter, setFilter] = useState('')
  const [sevFilter, setSevFilter] = useState<'all' | TableEvent['severity']>('all')
  const [open, setOpen] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(() => {
    api<TableEvent[]>('/api/events?limit=200')
      .then((data) => {
        setEvents(data)
        setError(null)
      })
      .catch((e: Error) => setError(e.message))
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(load, 10000)
    return () => clearInterval(id)
  }, [load])

  const visible = (events ?? []).filter(
    (e) =>
      (sevFilter === 'all' || e.severity === sevFilter) &&
      `${e.schemaName}.${e.tableName}`.toLowerCase().includes(filter.toLowerCase()),
  )

  return (
    <div className="mx-auto h-full max-w-4xl overflow-y-auto p-5">
      <div className="mb-4 flex items-center gap-2">
        <Input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="스키마.테이블 필터"
          className="w-56"
        />
        {(['all', 'INFO', 'WARN', 'ERROR'] as const).map((s) => (
          <Button
            key={s}
            size="sm"
            variant={sevFilter === s ? 'default' : 'ghost'}
            onClick={() => setSevFilter(s)}
          >
            {s === 'all' ? '전체' : s}
          </Button>
        ))}
        <span className="ml-auto font-mono text-[11px] text-muted-foreground">
          {visible.length} events
        </span>
      </div>

      {error && <p className="mb-3 text-sm text-destructive">{error}</p>}
      {events !== null && events.length === 0 && !error && (
        <p className="text-sm text-muted-foreground">
          아직 이벤트가 없습니다 — 등록·DDL·복구·커넥터 상태 변화가 여기에 쌓입니다.
        </p>
      )}

      <div className="relative pl-6">
        {visible.length > 0 && <div className="absolute bottom-1 left-2 top-1 w-0.5 bg-border" />}
        <div className="grid gap-2">
          {visible.map((e) => {
            const isOpen = open === e.id
            return (
              <div key={e.id} className="relative">
                <span
                  className="absolute -left-6 top-3 h-2.5 w-2.5 rounded-full border-2 border-background"
                  style={{ background: dotColor[e.severity] }}
                />
                <div
                  onClick={() => e.detail && setOpen(isOpen ? null : e.id)}
                  className={`rounded-xl border bg-card px-4 py-2.5 ${
                    e.detail ? 'cursor-pointer' : ''
                  } ${isOpen ? 'border-primary' : 'border-border'}`}
                >
                  <div className="flex flex-wrap items-center gap-3">
                    <span className="font-mono text-[11px] text-muted-foreground">
                      {e.occurredAt.replace('T', ' ').slice(0, 19)}
                    </span>
                    <span className="font-mono text-[12px] font-semibold">
                      {e.schemaName}.{e.tableName}
                    </span>
                    <span className="rounded bg-secondary px-1.5 py-0.5 font-mono text-[10px]">
                      {e.eventType}
                    </span>
                    <span className="text-[12px] text-muted-foreground">{e.message}</span>
                    <span
                      className={`ml-auto rounded px-2 py-0.5 font-mono text-[10px] ${sevColor[e.severity]}`}
                    >
                      {e.severity}
                    </span>
                  </div>
                  {isOpen && e.detail && (
                    <pre className="mt-2 max-h-64 overflow-auto rounded-lg border border-border bg-background p-3 font-mono text-[11px] leading-relaxed text-[#B9C7E4]">
                      {e.detail}
                    </pre>
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

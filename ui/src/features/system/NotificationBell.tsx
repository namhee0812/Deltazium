/**
 * 파일명 : NotificationBell.tsx
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : 헤더 알림 아이콘 — INFO 등급(사용자가 직접 정지했거나 DDL 거부로 apply만 멈춘
 * 커넥터)만 모아 보여준다. 경고 센터(WarningCenter)와 달리 고장이 아니므로 [확인] 버튼으로
 * 없앨 수 있다 — POST /api/system/warnings/{id}/ack 후 목록에서 제거. 아이콘은 항상
 * 표시하고 배지는 건수가 있을 때만(UI 최소주의). useSystemWarnings() 폴링 결과를
 * WarningCenter와 나눠 쓴다(중복 폴링 금지).
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 최초 생성 — 경고(고장)와 알림(사용자가 확인 후 없앨 수 있는
 * |                          | 정보성 항목)을 분리하라는 사용자 판단 반영
 * --------------------------------------------------
 */
import { useState } from 'react'
import { Bell } from 'lucide-react'
import { api } from '@/lib/api'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import type { SystemWarning } from './useSystemWarnings'
import { relativeTime } from './useSystemWarnings'

export function NotificationBell({ warnings: all }: { warnings: SystemWarning[] }) {
  // ack 요청을 보낸 즉시 로컬에서 숨긴다 — 다음 30초 폴링을 기다리지 않고 바로 반영.
  const [dismissed, setDismissed] = useState<Set<string>>(new Set())
  const [acking, setAcking] = useState<Set<string>>(new Set())

  const infos = all.filter((w) => w.severity === 'INFO' && !dismissed.has(w.id))

  const ack = (id: string) => {
    setAcking((s) => new Set(s).add(id))
    api(`/api/system/warnings/${encodeURIComponent(id)}/ack`, { method: 'POST' })
      .then(() => setDismissed((s) => new Set(s).add(id)))
      .catch(() => {
        // 실패해도 조용히 둔다 — 다음 폴링에서 여전히 뜨면 사용자가 다시 시도할 수 있다.
      })
      .finally(() => {
        setAcking((s) => {
          const next = new Set(s)
          next.delete(id)
          return next
        })
      })
  }

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          className="relative flex size-7 items-center justify-center rounded-md text-ink-2 hover:bg-surface-2 hover:text-foreground"
          title="알림"
        >
          <Bell className="size-4" />
          {infos.length > 0 && (
            <span className="absolute -right-0.5 -top-0.5 flex h-3.5 min-w-3.5 items-center justify-center rounded-full bg-primary px-0.5 font-mono text-[9px] leading-none text-primary-foreground">
              {infos.length}
            </span>
          )}
        </button>
      </PopoverTrigger>
      <PopoverContent align="end" className="max-h-96 w-80 overflow-y-auto">
        {infos.length === 0 ? (
          <p className="px-1 py-1.5 text-xs text-ink-3">알림이 없습니다.</p>
        ) : (
          <div className="flex flex-col gap-3">
            {infos.map((w) => (
              <div key={w.id} className="flex items-start gap-2">
                <span className="mt-1.5 size-2 shrink-0 rounded-full bg-ink-3" />
                <div className="min-w-0 flex-1">
                  <div className="text-[13px] font-medium text-foreground">{w.title}</div>
                  <div className="text-xs text-muted-foreground">{w.detail}</div>
                  {w.sinceMs !== null && (
                    <div className="mt-0.5 font-mono text-[10px] text-muted-foreground">
                      {relativeTime(w.sinceMs)}
                    </div>
                  )}
                </div>
                <button
                  className="shrink-0 rounded border border-line-2 px-2 py-1 text-[11px] font-medium text-ink-2 hover:border-primary hover:text-primary disabled:opacity-50"
                  disabled={acking.has(w.id)}
                  onClick={() => ack(w.id)}
                >
                  확인
                </button>
              </div>
            ))}
          </div>
        )}
      </PopoverContent>
    </Popover>
  )
}

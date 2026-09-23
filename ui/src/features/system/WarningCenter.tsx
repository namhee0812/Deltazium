/**
 * 파일명 : WarningCenter.tsx
 * 작성일자 : 26. 08. 24.
 * 작성자 : 최남희
 * 설명 : 전역 경고 센터 — 헤더 우측 칩 + 클릭 팝오버. CRITICAL·WARN만 집계한다
 * (INFO — 사용자가 정지했거나 DDL 거부로 apply만 멈춘 상태 — 는 확인 가능한 알림이라
 * NotificationBell이 따로 보여준다, 2026-09-23 분리). 경고가 없으면 아무것도 렌더하지
 * 않는다(UI 최소주의).
 *
 * 26-08-20 디스크 풀로 Kafka가 죽고 나흘간 미검출된 장애의 재발 방지가 목적이므로,
 * API 호출 자체가 실패하는 경우(backend 다운)에도 배너를 숨기지 않고 오히려
 * "backend 연결 끊김" 경고를 합성해 가장 눈에 띄게 띄운다(useSystemWarnings 훅 담당).
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 24.       | 최남희  | 최초 생성
 * 26. 08. 25.       | 최남희  | 합성 경고 문구를 사용자 용어로 — "backend"→"엔진", 복제 별개 동작 안내 추가
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 폴링을 useSystemWarnings 훅으로 분리(알림 아이콘과 응답 공유) —
 * |                          | 이 컴포넌트는 CRITICAL·WARN만 필터링해 보여준다
 * --------------------------------------------------
 */
import { AlertTriangle } from 'lucide-react'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import type { SystemWarning } from './useSystemWarnings'
import { relativeTime } from './useSystemWarnings'

export function WarningCenter({ warnings: all }: { warnings: SystemWarning[] }) {
  const warnings = all.filter((w) => w.severity === 'CRITICAL' || w.severity === 'WARN')

  if (warnings.length === 0) return null

  const worst = warnings.some((w) => w.severity === 'CRITICAL') ? 'CRITICAL' : 'WARN'
  const chipClass =
    worst === 'CRITICAL'
      ? 'border-crit/50 bg-crit/10 text-crit hover:bg-crit/20'
      : 'border-warn/50 bg-warn/10 text-warn hover:bg-warn/20'

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          className={`flex items-center gap-1.5 rounded-full border px-2.5 py-1 font-mono text-[11px] transition-colors ${chipClass}`}
          title="시스템 경고"
        >
          <AlertTriangle className="size-3.5" />
          {warnings.length}
        </button>
      </PopoverTrigger>
      <PopoverContent align="end" className="max-h-96 w-80 overflow-y-auto">
        <div className="flex flex-col gap-3">
          {warnings.map((w) => (
            <div key={w.id} className="flex items-start gap-2">
              <span
                className={`mt-1.5 size-2 shrink-0 rounded-full ${
                  w.severity === 'CRITICAL' ? 'bg-crit' : 'bg-warn'
                }`}
              />
              <div className="min-w-0 flex-1">
                <div className="text-[13px] font-medium text-foreground">{w.title}</div>
                <div className="text-xs text-muted-foreground">{w.detail}</div>
                {w.sinceMs !== null && (
                  <div className="mt-0.5 font-mono text-[10px] text-muted-foreground">
                    {relativeTime(w.sinceMs)}
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      </PopoverContent>
    </Popover>
  )
}

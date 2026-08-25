/**
 * 파일명 : WarningCenter.tsx
 * 작성일자 : 26. 08. 24.
 * 작성자 : 최남희
 * 설명 : 전역 경고 센터 — 헤더 우측 칩 + 클릭 팝오버. GET /api/system/warnings를
 * 30초 주기로 폴링해 디스크 사용률·Kafka 연결·커넥터 상태 경고를 모아 보여준다.
 * 경고가 없으면 아무것도 렌더하지 않는다(UI 최소주의).
 *
 * 26-08-20 디스크 풀로 Kafka가 죽고 나흘간 미검출된 장애의 재발 방지가 목적이므로,
 * 이 API 호출 자체가 실패하는 경우(backend 다운)에도 배너를 숨기지 않고 오히려
 * "backend 연결 끊김" 경고를 합성해 가장 눈에 띄게 띄운다.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 24.       | 최남희  | 최초 생성
 * 26. 08. 25.       | 최남희  | 합성 경고 문구를 사용자 용어로 — "backend"→"엔진", 복제 별개 동작 안내 추가
 * --------------------------------------------------
 */
import { useEffect, useRef, useState } from 'react'
import { AlertTriangle } from 'lucide-react'
import { api } from '@/lib/api'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'

interface SystemWarning {
  id: string
  severity: 'WARN' | 'CRITICAL'
  title: string
  detail: string
  sinceMs: number | null
}

interface WarningsResponse {
  warnings: SystemWarning[]
}

function relativeTime(ms: number): string {
  const diffMin = Math.floor((Date.now() - ms) / 60000)
  if (diffMin < 1) return '방금 전'
  if (diffMin < 60) return `${diffMin}분 전`
  const diffHour = Math.floor(diffMin / 60)
  if (diffHour < 24) return `${diffHour}시간 전`
  return `${Math.floor(diffHour / 24)}일 전`
}

export function WarningCenter() {
  const [warnings, setWarnings] = useState<SystemWarning[]>([])
  // API 호출 자체가 실패할 때 합성하는 "backend 연결 끊김" 경고의 클라이언트 측 최초 감지 시각.
  // 30초마다 새로 만들면 "방금 전"으로 계속 리셋돼 장기 장애가 방금 시작한 것처럼 보이므로 보존한다.
  const unreachableSince = useRef<number | null>(null)

  useEffect(() => {
    const load = () => {
      api<WarningsResponse>('/api/system/warnings')
        .then((res) => {
          unreachableSince.current = null
          setWarnings(res.warnings)
        })
        .catch((e: Error) => {
          if (unreachableSince.current === null) {
            unreachableSince.current = Date.now()
          }
          setWarnings([
            {
              id: 'backend-unreachable',
              severity: 'CRITICAL',
              title: '엔진 연결 끊김',
              detail: `관제 화면이 Deltazium 엔진에 연결하지 못했습니다. 복제 파이프라인(Kafka·Connect)은 이 화면과 별개로 동작 중일 수 있습니다. (${e.message})`,
              sinceMs: unreachableSince.current,
            },
          ])
        })
    }
    load()
    const id = setInterval(load, 30000)
    return () => clearInterval(id)
  }, [])

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

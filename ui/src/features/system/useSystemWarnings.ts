/**
 * 파일명 : useSystemWarnings.ts
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : GET /api/system/warnings 30초 폴링 훅 — 헤더의 경고 센터(WarningCenter)와
 * 알림(NotificationBell) 두 컴포넌트가 같은 응답을 나눠 쓴다(중복 폴링 금지, 상위인
 * App이 한 번만 호출해 내려준다). severity는 CRITICAL/WARN/INFO 3단(backend
 * SystemWarningService, docs/internals.md) — INFO는 사용자가 정지했거나 DDL 거부로
 * apply만 멈춘 상태로, 경고가 아니라 확인(ack) 가능한 알림이다.
 *
 * 26-08-20 디스크 풀로 Kafka가 죽고 나흘간 미검출된 장애의 재발 방지가 목적이므로,
 * 이 API 호출 자체가 실패하는 경우(backend 다운)에도 조용히 넘기지 않고 오히려
 * "backend 연결 끊김"을 CRITICAL로 합성해 가장 눈에 띄게 띄운다.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | WarningCenter.tsx에서 폴링 로직을 분리 — 알림(INFO) 아이콘과
 * |                          | 경고 칩이 폴링을 나눠 쓰도록
 * --------------------------------------------------
 */
import { useEffect, useRef, useState } from 'react'
import { api } from '@/lib/api'

export interface SystemWarning {
  id: string
  severity: 'CRITICAL' | 'WARN' | 'INFO'
  title: string
  detail: string
  sinceMs: number | null
}

interface WarningsResponse {
  warnings: SystemWarning[]
}

export function useSystemWarnings(): SystemWarning[] {
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

  return warnings
}

export function relativeTime(ms: number): string {
  const diffMin = Math.floor((Date.now() - ms) / 60000)
  if (diffMin < 1) return '방금 전'
  if (diffMin < 60) return `${diffMin}분 전`
  const diffHour = Math.floor(diffMin / 60)
  if (diffHour < 24) return `${diffHour}시간 전`
  return `${Math.floor(diffHour / 24)}일 전`
}

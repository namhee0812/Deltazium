/**
 * 파일명 : useSettings.ts
 * 작성일자 : 26. 09. 27.
 * 작성자 : 최남희
 * 설명 : GET /api/system/settings 마운트 1회 조회 훅 — lag 경고 임계 등 대시보드·테이블
 * 모니터링 화면이 각자 하드코딩(LAG_WARN=100)하던 값을 backend 설정(application.yml
 * deltazium.lag-warn-records) 하나로 모은다. 정적 설정이라 폴링하지 않는다. 조회 실패
 * (구버전 backend 등 API 미존재 포함) 시 기존 하드코딩과 같은 기본값으로 조용히 폴백한다.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 최초 생성 — TopologyPanel·TablesPanel의 LAG_WARN 하드코딩 제거
 * --------------------------------------------------
 */
import { useEffect, useState } from 'react'
import { api } from '@/lib/api'

export interface SystemSettings {
  lagWarnRecords: number
}

/** API 조회 전·실패 시 기본값 — 기존 하드코딩(LAG_WARN=100)과 동일해 화면이 변하지 않는다. */
export const DEFAULT_SETTINGS: SystemSettings = { lagWarnRecords: 100 }

export function useSettings(): SystemSettings {
  const [settings, setSettings] = useState<SystemSettings>(DEFAULT_SETTINGS)

  useEffect(() => {
    api<SystemSettings>('/api/system/settings')
      .then(setSettings)
      .catch(() => setSettings(DEFAULT_SETTINGS))
  }, [])

  return settings
}

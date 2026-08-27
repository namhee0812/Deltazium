/**
 * 파일명 : theme.ts
 * 작성일자 : 26. 08. 27.
 * 작성자 : 최남희
 * 설명 : 다크/라이트 테마 상태 훅. 초기값 결정 순서 — localStorage('dz-theme') →
 * matchMedia(prefers-color-scheme) → 기본값(dark). 실제 적용(document.documentElement의
 * .dark 클래스 토글)은 index.html의 인라인 스크립트가 React 마운트 전에 먼저 수행해
 * FOUC를 막고, 이 훅은 이후 토글 상호작용과 상태 동기화만 담당한다.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 27.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
import { useCallback, useEffect, useState } from 'react'

export type Theme = 'dark' | 'light'

const STORAGE_KEY = 'dz-theme'

function readInitialTheme(): Theme {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored === 'dark' || stored === 'light') return stored
  } catch {
    // localStorage 접근 차단 환경(프라이빗 모드 등) — matchMedia로 폴백
  }
  try {
    if (window.matchMedia('(prefers-color-scheme: dark)').matches) return 'dark'
    if (window.matchMedia('(prefers-color-scheme: light)').matches) return 'light'
  } catch {
    // matchMedia 미지원 — 최종 기본값으로 폴백
  }
  return 'dark'
}

function applyTheme(theme: Theme) {
  document.documentElement.classList.toggle('dark', theme === 'dark')
  try {
    localStorage.setItem(STORAGE_KEY, theme)
  } catch {
    // 저장 실패는 무시 — 이번 세션 동안의 표시에는 영향 없음
  }
}

export function useTheme() {
  const [theme, setThemeState] = useState<Theme>(() => readInitialTheme())

  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  const setTheme = useCallback((t: Theme) => setThemeState(t), [])
  const toggle = useCallback(() => setThemeState((t) => (t === 'dark' ? 'light' : 'dark')), [])

  return { theme, setTheme, toggle }
}

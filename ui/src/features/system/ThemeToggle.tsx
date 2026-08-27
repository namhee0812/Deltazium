/**
 * 파일명 : ThemeToggle.tsx
 * 작성일자 : 26. 08. 27.
 * 작성자 : 최남희
 * 설명 : 다크/라이트 테마 토글 — 해/달 스위치. uiverse.io(andrew-demchenk0) 원안을
 * 헤더 높이에 맞춰 축소 이식(48×26, 노브 22px, 아이콘 18px). 장식 요소라 하늘·밤·노브
 * 색상은 원안 리터럴을 그대로 쓰고 디자인 토큰과는 별개로 취급한다(index.css 참조).
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 27.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
import { Moon, Sun } from 'lucide-react'
import { useTheme } from '@/lib/theme'

export function ThemeToggle() {
  const { theme, toggle } = useTheme()
  const dark = theme === 'dark'

  return (
    <label className="theme-switch" aria-label="다크/라이트 전환">
      <input
        type="checkbox"
        className="theme-switch__input"
        checked={dark}
        onChange={toggle}
      />
      <span className="theme-switch__slider" />
      <span className="theme-switch__sun">
        <Sun size={18} strokeWidth={2} />
      </span>
      <span className="theme-switch__moon">
        <Moon size={18} strokeWidth={2} />
      </span>
    </label>
  )
}

/**
 * 파일명 : main.tsx
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : React 엔트리포인트 — 루트 렌더링.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)

/**
 * 파일명 : App.tsx
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : 콘솔 루트 — 좌측 rail 내비게이션(대시보드/테이블/DDL/이벤트/복구/DB 연결) +
 * 상단 바(페이지 제목·부제·우측 액션), 엔진 상태 요약, 등록 위저드 마운트.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 상단 요약 running 집계를 effectiveState(connector+task) 기준으로 교체
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 토폴로지 탭 → 대시보드로 개명 (토폴로지+차트+자원 통합)
 * --------------------------------------------------
 * 26. 08. 13.       | 최남희  | AI 진단 탭 추가
 * --------------------------------------------------
 * 26. 08. 14.       | 최남희  | AI 진단을 상단 탭에서 우하단 플로팅 위젯(AssistWidget)으로 전환
 * --------------------------------------------------
 * 26. 08. 24.       | 최남희  | 헤더 우측에 전역 경고 센터(WarningCenter) 칩 추가
 * --------------------------------------------------
 * 26. 08. 27.       | 최남희  | 헤더에 다크/라이트 테마 토글 추가, "backend"→"엔진" 용어 통일
 * --------------------------------------------------
 * 26. 08. 28.       | 최남희  | 상단 탭 → 좌측 rail(220px) + 상단 바(56px) 리디자인.
 * |                          | rail에 DDL 미승인 건수 배지·엔진 상태 pill, 상단 바는
 * |                          | 페이지 제목/부제 + 우측 액션(등록·테마·경고 센터)만 남김
 * --------------------------------------------------
 * 26. 08. 28.       | 최남희  | TopologyPanel에 onNavigate 전달 — 대시보드 "주의 필요"·KPI
 * |                          | 카드의 "보기/검토" 클릭 시 해당 탭으로 이동
 * --------------------------------------------------
 */
import { useEffect, useState } from 'react'
import {
  Clock,
  Database,
  History,
  LayoutDashboard,
  Mountain,
  RotateCcw,
  Table2,
} from 'lucide-react'
import { api } from '@/lib/api'
import { effectiveState } from '@/lib/connect'
import type { ConnectorStates } from '@/lib/connect'
import { AssistWidget } from '@/features/assist/AssistWidget'
import { ConnectionsPanel } from '@/features/connections/ConnectionsPanel'
import { DdlPanel } from '@/features/ddl/DdlPanel'
import { EventsPanel } from '@/features/events/EventsPanel'
import { RecoveryPanel } from '@/features/recovery/RecoveryPanel'
import { RegistrationWizard } from '@/features/registration/RegistrationWizard'
import { TablesPanel } from '@/features/tables/TablesPanel'
import { TopologyPanel } from '@/features/topology/TopologyPanel'
import { ThemeToggle } from '@/features/system/ThemeToggle'
import { WarningCenter } from '@/features/system/WarningCenter'
import { Button } from '@/components/ui/button'
import { StatusPill } from '@/components/ui/status-pill'

type View = 'topology' | 'tables' | 'ddl' | 'events' | 'recovery' | 'connections'

const VIEWS: { key: View; label: string; sub: string; icon: typeof LayoutDashboard }[] = [
  { key: 'topology', label: '대시보드', sub: '파이프라인 전체 흐름', icon: LayoutDashboard },
  { key: 'tables', label: '테이블 모니터링', sub: '실측 이벤트·lag 모니터링', icon: Table2 },
  { key: 'ddl', label: 'DDL 이력', sub: '소스 DDL 승인 타임라인', icon: History },
  { key: 'events', label: '이벤트', sub: '운영 이벤트 이력', icon: Clock },
  { key: 'recovery', label: '복구', sub: 'changelog 재발행 · 정합 검증', icon: RotateCcw },
  { key: 'connections', label: 'DB 연결', sub: '소스·타깃 연결 관리', icon: Database },
]

interface DdlEventLite {
  state: 'SNAPSHOT' | 'DETECTED' | 'APPROVED' | 'REJECTED' | 'IGNORED'
}

function App() {
  const [view, setView] = useState<View>('topology')
  const [summary, setSummary] = useState<{ running: number; total: number } | null>(null)
  const [pendingDdl, setPendingDdl] = useState(0)
  const [wizardOpen, setWizardOpen] = useState(false)
  const [refreshKey, setRefreshKey] = useState(0)

  useEffect(() => {
    const load = () =>
      api<ConnectorStates>('/api/connectors')
        .then((cs) => {
          const states = Object.values(cs).map((c) => effectiveState(c))
          setSummary({
            running: states.filter((s) => s === 'RUNNING').length,
            total: states.length,
          })
        })
        .catch(() => setSummary(null))
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [])

  useEffect(() => {
    const load = () =>
      api<DdlEventLite[]>('/api/ddl-events')
        .then((evs) => setPendingDdl(evs.filter((e) => e.state === 'DETECTED').length))
        .catch(() => {})
    load()
    const id = setInterval(load, 10000)
    return () => clearInterval(id)
  }, [])

  const current = VIEWS.find((v) => v.key === view)!

  return (
    <div className="flex h-screen bg-background text-foreground">
      {/* 좌측 rail */}
      <aside className="flex w-[220px] shrink-0 flex-col gap-1 bg-rail px-3 py-4 text-rail-ink-2">
        <div className="flex items-center gap-2.5 px-2 pb-4.5 pt-1">
          <div className="flex size-7 items-center justify-center rounded-md bg-primary">
            <Mountain className="size-4 text-white" strokeWidth={2.2} />
          </div>
          <div className="flex flex-col leading-tight">
            <span className="text-sm font-bold tracking-tight text-rail-ink">
              Deltazium
            </span>
            <span className="font-mono text-[10.5px] text-rail-ink-2">CDC console</span>
          </div>
        </div>

        {VIEWS.map((v) => {
          const Icon = v.icon
          const active = view === v.key
          return (
            <button
              key={v.key}
              onClick={() => setView(v.key)}
              className={`flex h-9 items-center gap-2.5 rounded-md px-3 text-[13px] transition-colors ${
                active
                  ? 'bg-rail-2 font-semibold text-rail-ink shadow-[inset_3px_0_0_var(--brand-2)]'
                  : 'text-rail-ink-2 hover:bg-rail-2 hover:text-rail-ink'
              }`}
            >
              <Icon className="size-4 shrink-0" strokeWidth={1.8} />
              {v.label}
              {v.key === 'ddl' && pendingDdl > 0 && (
                <StatusPill variant="warn" className="ml-auto h-[18px]">
                  {pendingDdl}
                </StatusPill>
              )}
            </button>
          )
        })}

        <div className="mt-auto flex flex-col gap-2 border-t border-rail-2 pt-3">
          <div className="flex items-center gap-2 px-1 text-xs text-rail-ink-2">
            {summary === null ? (
              <StatusPill variant="crit">엔진</StatusPill>
            ) : (
              <>
                <StatusPill variant="ok">엔진</StatusPill>
                <span className="font-mono text-[11px]">
                  커넥터 {summary.running}/{summary.total} running
                </span>
              </>
            )}
          </div>
        </div>
      </aside>

      {/* 본문 */}
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 shrink-0 items-center gap-3 border-b border-border bg-card px-6">
          <span className="text-[15px] font-semibold">{current.label}</span>
          <span className="text-xs text-ink-3">{current.sub}</span>
          <div className="ml-auto flex items-center gap-2">
            <Button size="sm" onClick={() => setWizardOpen(true)}>
              ＋ CDC 등록
            </Button>
            <ThemeToggle />
            <WarningCenter />
          </div>
        </header>

        <main className="min-h-0 flex-1">
          {view === 'topology' && <TopologyPanel onNavigate={setView} />}
          {view === 'tables' && <TablesPanel refreshKey={refreshKey} />}
          {view === 'ddl' && <DdlPanel />}
          {view === 'events' && <EventsPanel />}
          {view === 'recovery' && <RecoveryPanel />}
          {view === 'connections' && (
            <div className="mx-auto max-w-4xl p-6">
              <ConnectionsPanel />
            </div>
          )}
        </main>
      </div>

      <RegistrationWizard
        open={wizardOpen}
        onClose={() => setWizardOpen(false)}
        onRegistered={() => {
          setRefreshKey((k) => k + 1)
          setView('tables')
        }}
      />

      <AssistWidget />
    </div>
  )
}

export default App

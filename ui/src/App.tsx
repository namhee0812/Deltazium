/**
 * 파일명 : App.tsx
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : 콘솔 루트 — 상단 탭 내비게이션(토폴로지/테이블/DDL/이벤트/복구/DB 연결)과 커넥터 상태 요약, 등록 위저드 마운트.
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
 */
import { useEffect, useState } from 'react'
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

type View = 'topology' | 'tables' | 'ddl' | 'events' | 'recovery' | 'connections'

const VIEWS: [View, string][] = [
  ['topology', '대시보드'],
  ['tables', '테이블 모니터링'],
  ['ddl', 'DDL 이력'],
  ['events', '이벤트'],
  ['recovery', '복구'],
  ['connections', 'DB 연결'],
]

function App() {
  const [view, setView] = useState<View>('topology')
  const [summary, setSummary] = useState<{ running: number; failed: number } | null>(null)
  const [wizardOpen, setWizardOpen] = useState(false)
  const [refreshKey, setRefreshKey] = useState(0)

  useEffect(() => {
    const load = () =>
      api<ConnectorStates>('/api/connectors')
        .then((cs) => {
          const states = Object.values(cs).map((c) => effectiveState(c))
          setSummary({
            running: states.filter((s) => s === 'RUNNING').length,
            failed: states.filter((s) => s !== 'RUNNING').length,
          })
        })
        .catch(() => setSummary(null))
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [])

  return (
    <div className="flex h-screen flex-col bg-background text-foreground">
      <header className="flex items-center gap-5 border-b border-border bg-card px-5 py-2.5">
        <div className="font-semibold tracking-tight">
          Delta<span className="text-primary">zium</span>{' '}
          <span className="font-normal text-muted-foreground">Console</span>
        </div>
        <nav className="flex gap-4">
          {VIEWS.map(([k, label]) => (
            <button
              key={k}
              onClick={() => setView(k)}
              className={`border-b-2 px-0.5 py-1 text-[13px] transition-colors ${
                view === k
                  ? 'border-primary font-semibold text-foreground'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              }`}
            >
              {label}
            </button>
          ))}
        </nav>
        <Button size="sm" className="ml-auto" onClick={() => setWizardOpen(true)}>
          ＋ CDC 등록
        </Button>
        <div className="flex gap-4 font-mono text-xs text-muted-foreground">
          {summary === null ? (
            <span className="text-crit">● backend 연결 안 됨</span>
          ) : (
            <>
              <span title="Kafka Connect 커넥터 수 (테이블 수 아님)">
                <span className="text-ok">●</span> 커넥터 {summary.running} running
              </span>
              {summary.failed > 0 && (
                <span>
                  <span className="text-crit">●</span> {summary.failed} not running
                </span>
              )}
            </>
          )}
        </div>
        <ThemeToggle />
        <WarningCenter />
      </header>

      <main className="min-h-0 flex-1">
        {view === 'topology' && <TopologyPanel />}
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

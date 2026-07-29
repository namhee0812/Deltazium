import { useEffect, useState } from 'react'
import { api } from '@/lib/api'
import { ConnectionsPanel } from '@/features/connections/ConnectionsPanel'
import { DdlPanel } from '@/features/ddl/DdlPanel'
import { EventsPanel } from '@/features/events/EventsPanel'
import { RecoveryPanel } from '@/features/recovery/RecoveryPanel'
import { RegistrationWizard } from '@/features/registration/RegistrationWizard'
import { TablesPanel } from '@/features/tables/TablesPanel'
import { TopologyPanel } from '@/features/topology/TopologyPanel'
import { Button } from '@/components/ui/button'

type View = 'topology' | 'tables' | 'ddl' | 'events' | 'recovery' | 'connections'

const VIEWS: [View, string][] = [
  ['topology', '토폴로지'],
  ['tables', '테이블 모니터링'],
  ['ddl', 'DDL 이력'],
  ['events', '이벤트'],
  ['recovery', '복구'],
  ['connections', 'DB 연결'],
]

type ConnectorStates = Record<string, { status?: { connector?: { state?: string } } }>

function App() {
  const [view, setView] = useState<View>('topology')
  const [summary, setSummary] = useState<{ running: number; failed: number } | null>(null)
  const [wizardOpen, setWizardOpen] = useState(false)
  const [refreshKey, setRefreshKey] = useState(0)

  useEffect(() => {
    const load = () =>
      api<ConnectorStates>('/api/connectors')
        .then((cs) => {
          const states = Object.values(cs).map((c) => c.status?.connector?.state)
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
              <span>
                <span className="text-ok">●</span> {summary.running} running
              </span>
              {summary.failed > 0 && (
                <span>
                  <span className="text-crit">●</span> {summary.failed} not running
                </span>
              )}
            </>
          )}
        </div>
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
    </div>
  )
}

export default App

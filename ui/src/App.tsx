import { useEffect, useState } from 'react'
import { api } from '@/lib/api'
import { ConnectionsPanel } from '@/features/connections/ConnectionsPanel'
import { DdlPanel } from '@/features/ddl/DdlPanel'
import { TablesPanel } from '@/features/tables/TablesPanel'
import { TopologyPanel } from '@/features/topology/TopologyPanel'

type View = 'topology' | 'tables' | 'ddl' | 'connections'

const VIEWS: [View, string][] = [
  ['topology', '토폴로지'],
  ['tables', '테이블 모니터링'],
  ['ddl', 'DDL 이력'],
  ['connections', 'DB 연결'],
]

type ConnectorStates = Record<string, { status?: { connector?: { state?: string } } }>

function App() {
  const [view, setView] = useState<View>('topology')
  const [summary, setSummary] = useState<{ running: number; failed: number } | null>(null)

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
        <div className="ml-auto flex gap-4 font-mono text-xs text-muted-foreground">
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
        {view === 'tables' && <TablesPanel />}
        {view === 'ddl' && <DdlPanel />}
        {view === 'connections' && (
          <div className="mx-auto max-w-4xl p-6">
            <ConnectionsPanel />
          </div>
        )}
      </main>
    </div>
  )
}

export default App

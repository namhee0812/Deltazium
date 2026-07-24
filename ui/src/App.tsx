import { useEffect, useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'

type ConnectorStates = Record<string, { status?: { connector?: { state?: string } } }>

/**
 * 최소 셸 — 실제 화면(토폴로지 캔버스·테이블 그리드·DDL 타임라인)은
 * ui-reference/ 프로토타입 기준으로 마일스톤 7에서 구현한다.
 */
function App() {
  const [connectors, setConnectors] = useState<ConnectorStates | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = () => {
    setError(null)
    fetch('/api/connectors')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(setConnectors)
      .catch((e: Error) => setError(e.message))
  }

  useEffect(load, [])

  return (
    <div className="min-h-screen bg-background p-8">
      <div className="mx-auto max-w-3xl space-y-6">
        <header className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">Deltazium</h1>
            <p className="text-sm text-muted-foreground">
              Debezium + Kafka CDC 파이프라인 제어면
            </p>
          </div>
          <Button variant="outline" size="sm" onClick={load}>
            새로고침
          </Button>
        </header>

        <Card>
          <CardHeader>
            <CardTitle>커넥터 상태</CardTitle>
            <CardDescription>backend(/api/connectors) 경유 Connect 조회</CardDescription>
          </CardHeader>
          <CardContent>
            {error && (
              <p className="text-sm text-destructive">
                backend에 연결할 수 없습니다: {error}
              </p>
            )}
            {!error && connectors && Object.keys(connectors).length === 0 && (
              <p className="text-sm text-muted-foreground">배포된 커넥터가 없습니다.</p>
            )}
            {!error && connectors && (
              <ul className="space-y-2">
                {Object.entries(connectors).map(([name, info]) => (
                  <li key={name} className="flex items-center gap-2">
                    <span className="font-mono text-sm">{name}</span>
                    <Badge
                      variant={
                        info.status?.connector?.state === 'RUNNING'
                          ? 'default'
                          : 'destructive'
                      }
                    >
                      {info.status?.connector?.state ?? 'UNKNOWN'}
                    </Badge>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

export default App

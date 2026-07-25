import { useEffect, useState } from 'react'
import { ConnectionsPanel } from '@/features/connections/ConnectionsPanel'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'

type ConnectorStates = Record<string, { status?: { connector?: { state?: string } } }>

/**
 * 셸 + 탭 구성. 토폴로지 캔버스·테이블 그리드·DDL 타임라인(ui-reference 기준)은
 * backend API가 갖춰지는 대로 마일스톤 7에서 구현한다.
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
      <div className="mx-auto max-w-4xl space-y-6">
        <header>
          <h1 className="text-2xl font-semibold tracking-tight">Deltazium</h1>
          <p className="text-sm text-muted-foreground">
            Debezium + Kafka CDC 파이프라인 제어면
          </p>
        </header>

        <Tabs defaultValue="connections">
          <TabsList>
            <TabsTrigger value="connections">DB 연결</TabsTrigger>
            <TabsTrigger value="connectors">커넥터</TabsTrigger>
          </TabsList>

          <TabsContent value="connections">
            <Card>
              <CardHeader>
                <CardTitle>DB 연결 저장소</CardTitle>
                <CardDescription>현재 Oracle 지원 — 종류는 점차 확장</CardDescription>
              </CardHeader>
              <CardContent>
                <ConnectionsPanel />
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="connectors">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  커넥터 상태
                  <Button variant="outline" size="sm" onClick={load}>
                    새로고침
                  </Button>
                </CardTitle>
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
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}

export default App

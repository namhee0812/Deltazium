import { useEffect, useMemo, useState } from 'react'
import { Background, Handle, Position, ReactFlow } from '@xyflow/react'
import type { Edge, Node, NodeProps } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { api } from '@/lib/api'
import type { DbConnection } from '@/features/connections/types'

/* Deltazium 파이프라인 토폴로지 (architecture.md 2절) — 커넥터·DB 연결은 실데이터 */

const COLOR = { ok: '#56D89C', warn: '#F5B453', crit: '#F0647A', dim: '#8A97B4', accent: '#53C8E8' }

type NodeStatus = 'ok' | 'warn' | 'crit' | 'none'

interface DzNodeData extends Record<string, unknown> {
  label: string
  sub: string
  status: NodeStatus
}

function statusColor(s: NodeStatus) {
  return s === 'none' ? COLOR.dim : COLOR[s]
}

function DzNode({ data, selected }: NodeProps) {
  const d = data as DzNodeData
  return (
    <div
      className="rounded-[10px] border bg-surface2 px-3 py-2.5"
      style={{ width: 168, borderColor: selected ? COLOR.accent : '#26334F' }}
    >
      <Handle type="target" position={Position.Left} className="opacity-0" />
      <div className="flex items-center gap-2">
        <span
          className="inline-block h-2 w-2 rounded-full"
          style={{ background: statusColor(d.status) }}
        />
        <span className="text-[13px] font-semibold text-foreground">{d.label}</span>
      </div>
      <div className="mt-1 font-mono text-[10px] text-muted-foreground">{d.sub}</div>
      <Handle type="source" position={Position.Right} className="opacity-0" />
    </div>
  )
}

const nodeTypes = { dz: DzNode }

type ConnectorStates = Record<string, { status?: { connector?: { state?: string } } }>

function connectorStatus(states: ConnectorStates | null, name: string): NodeStatus {
  if (!states || !(name in states)) return 'none'
  const s = states[name].status?.connector?.state
  return s === 'RUNNING' ? 'ok' : s === 'PAUSED' ? 'warn' : 'crit'
}

/** jdbc-sink는 테이블별 커넥터(dz-jdbc-sink-*) — 최악 상태로 집계 */
function jdbcSinkAggregate(states: ConnectorStates | null): { status: NodeStatus; count: number } {
  if (!states) return { status: 'none', count: 0 }
  const sinks = Object.entries(states).filter(([n]) => n.startsWith('dz-jdbc-sink-'))
  if (sinks.length === 0) return { status: 'none', count: 0 }
  const st = sinks.map(([, i]) => i.status?.connector?.state)
  const status: NodeStatus = st.some((s) => s !== 'RUNNING' && s !== 'PAUSED')
    ? 'crit'
    : st.some((s) => s === 'PAUSED')
      ? 'warn'
      : 'ok'
  return { status, count: sinks.length }
}

export function TopologyPanel() {
  const [connectors, setConnectors] = useState<ConnectorStates | null>(null)
  const [connections, setConnections] = useState<DbConnection[]>([])

  useEffect(() => {
    const load = () => {
      api<ConnectorStates>('/api/connectors').then(setConnectors).catch(() => setConnectors(null))
      api<DbConnection[]>('/api/connections').then(setConnections).catch(() => setConnections([]))
    }
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [])

  const { nodes, edges } = useMemo(() => {
    const source = connections.find((c) => c.role === 'SOURCE')
    const target = connections.find((c) => c.role === 'TARGET')
    const deployed = connectors !== null && Object.keys(connectors).length > 0

    const nodes: Node[] = [
      {
        id: 'oracle-src', type: 'dz', position: { x: 0, y: 150 },
        data: {
          label: source ? source.name : 'Oracle SRC',
          sub: source ? `${source.host}:${source.port}/${source.databaseName}` : '연결 미등록',
          status: source ? 'ok' : 'none',
        } satisfies DzNodeData,
      },
      {
        id: 'dz-source', type: 'dz', position: { x: 230, y: 150 },
        data: {
          label: 'dz-source',
          sub: 'Debezium Oracle · LogMiner',
          status: connectorStatus(connectors, 'dz-source'),
        } satisfies DzNodeData,
      },
      {
        id: 'kafka', type: 'dz', position: { x: 460, y: 150 },
        data: {
          label: 'Kafka',
          sub: 'KRaft · 테이블당 토픽 1개',
          status: connectors === null ? 'crit' : 'ok',
        } satisfies DzNodeData,
      },
      {
        id: 'jdbc-sink', type: 'dz', position: { x: 690, y: 60 },
        data: {
          label: 'dz-jdbc-sink',
          sub: jdbcSinkAggregate(connectors).count > 0
            ? `PK upsert · 테이블별 ${jdbcSinkAggregate(connectors).count}개`
            : 'PK upsert · 실 적재',
          status: jdbcSinkAggregate(connectors).status,
        } satisfies DzNodeData,
      },
      {
        id: 'iceberg-sink', type: 'dz', position: { x: 690, y: 240 },
        data: {
          label: 'dz-iceberg-sink',
          sub: 'append-only changelog',
          status: connectorStatus(connectors, 'dz-iceberg-sink'),
        } satisfies DzNodeData,
      },
      {
        id: 'oracle-tgt', type: 'dz', position: { x: 920, y: 60 },
        data: {
          label: target ? target.name : 'Oracle TGT',
          sub: target ? `${target.host}:${target.port}/${target.databaseName}` : '연결 미등록',
          status: target ? 'ok' : 'none',
        } satisfies DzNodeData,
      },
      {
        id: 'iceberg', type: 'dz', position: { x: 920, y: 240 },
        data: {
          label: 'Iceberg / MinIO',
          sub: 'changelog · 복구 원본',
          status: deployed ? 'ok' : 'none',
        } satisfies DzNodeData,
      },
      {
        id: 'recovery', type: 'dz', position: { x: 690, y: 380 },
        data: {
          label: 'recovery-job',
          sub: 'scan → 재발행 (평시 정지)',
          status: 'none',
        } satisfies DzNodeData,
      },
    ]

    const edge = (id: string, s: string, t: string, animated: boolean, dashed = false): Edge => ({
      id, source: s, target: t, animated,
      style: {
        stroke: animated ? COLOR.accent : '#26334F',
        strokeWidth: 2,
        ...(dashed ? { strokeDasharray: '6 6' } : {}),
      },
    })

    const running = (name: string) => connectorStatus(connectors, name) === 'ok'
    const jdbcRunning = jdbcSinkAggregate(connectors).status === 'ok'
    const edges: Edge[] = [
      edge('e1', 'oracle-src', 'dz-source', running('dz-source')),
      edge('e2', 'dz-source', 'kafka', running('dz-source')),
      edge('e3', 'kafka', 'jdbc-sink', jdbcRunning),
      edge('e4', 'kafka', 'iceberg-sink', running('dz-iceberg-sink')),
      edge('e5', 'jdbc-sink', 'oracle-tgt', jdbcRunning),
      edge('e6', 'iceberg-sink', 'iceberg', running('dz-iceberg-sink')),
      edge('e7', 'iceberg', 'recovery', false, true),
      edge('e8', 'recovery', 'kafka', false, true),
    ]
    return { nodes, edges }
  }, [connectors, connections])

  return (
    <div className="h-full min-h-[480px]">
      {connectors === null && (
        <div className="border-b border-border bg-card px-4 py-2 text-xs text-warn">
          backend(8090)에 연결할 수 없습니다 — 상태는 표시용 기본값입니다.
        </div>
      )}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        proOptions={{ hideAttribution: true }}
        nodesDraggable={false}
        nodesConnectable={false}
        colorMode="dark"
      >
        <Background color="#26334F" gap={24} />
      </ReactFlow>
    </div>
  )
}

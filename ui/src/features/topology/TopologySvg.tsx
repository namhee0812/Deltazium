/**
 * 파일명 : TopologySvg.tsx
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 파이프라인 토폴로지 — 자체 SVG (React Flow 대체).
 * 고정 8노드 정적 다이어그램이라 직접 그린다: 직교 라우팅으로 교차 없음,
 * RUNNING 간선은 점선 흐름 애니메이션, 뷰포트 상태가 없어 사라짐 이슈도 없음.
 * 레인: 상단(타깃 적재) / 중단(캡처 본류) / 하단(changelog) / 최하단(복구, 점선).
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성 (React Flow 제거)
 * --------------------------------------------------
 * 26. 08. 27.       | 최남희  | 하드코딩 hex를 CSS 변수(var(--ok) 등)로 교체 — 라이트 테마 대응
 * --------------------------------------------------
 */

export type NodeStatus = 'ok' | 'warn' | 'crit' | 'none'

export interface TopoNode {
  label: string
  sub: string
  status: NodeStatus
}

export interface TopoData {
  srcDb: TopoNode
  source: TopoNode
  kafka: TopoNode
  jdbcSink: TopoNode
  targetDb: TopoNode
  icebergSink: TopoNode
  iceberg: TopoNode
  recovery: TopoNode
}

const STATUS_COLOR: Record<NodeStatus, string> = {
  ok: 'var(--ok)',
  warn: 'var(--warn)',
  crit: 'var(--crit)',
  none: 'var(--chart-dim)',
}

const W = 150
const H = 54

/** 노드 좌표 (좌상단) — 레인 배치라 간선이 교차하지 않는다 */
const POS = {
  srcDb: [20, 123],
  source: [205, 123],
  kafka: [390, 123],
  jdbcSink: [575, 30],
  targetDb: [770, 30],
  icebergSink: [575, 216],
  iceberg: [770, 216],
  recovery: [575, 312],
} as const

function Node({ id, n }: { id: keyof typeof POS; n: TopoNode }) {
  const [x, y] = POS[id]
  return (
    <g>
      <rect x={x} y={y} width={W} height={H} rx={10}
        fill="var(--surface2)" stroke="var(--chart-grid)" strokeWidth="1" />
      <circle cx={x + 16} cy={y + 19} r={4} fill={STATUS_COLOR[n.status]}>
        {n.status === 'crit' && (
          <animate attributeName="opacity" values="1;0.25;1" dur="1.2s" repeatCount="indefinite" />
        )}
      </circle>
      <text x={x + 28} y={y + 23} fontSize="12.5" fontWeight="600" fill="var(--foreground)">
        {n.label.length > 16 ? n.label.slice(0, 15) + '…' : n.label}
      </text>
      <text x={x + 16} y={y + 41} fontSize="9.5" fontFamily="monospace" fill="var(--chart-dim)">
        {n.sub.length > 26 ? n.sub.slice(0, 25) + '…' : n.sub}
      </text>
    </g>
  )
}

/** 직교 경로 — 점 목록을 꺾인선으로 */
function ortho(points: [number, number][]): string {
  return points.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x},${y}`).join(' ')
}

function EdgePath({ d, active, dashed }: { d: string; active: boolean; dashed?: boolean }) {
  return (
    <g>
      <path d={d} fill="none" stroke={active ? 'var(--accent-cyan)' : 'var(--chart-grid)'} strokeWidth="2"
        strokeDasharray={dashed ? '5 5' : active ? '7 5' : undefined}
        className={active && !dashed ? 'topo-flow' : undefined} />
    </g>
  )
}

export function TopologySvg({ data }: { data: TopoData }) {
  const right = (id: keyof typeof POS): [number, number] =>
    [POS[id][0] + W, POS[id][1] + H / 2]
  const left = (id: keyof typeof POS): [number, number] =>
    [POS[id][0], POS[id][1] + H / 2]
  const bottom = (id: keyof typeof POS): [number, number] =>
    [POS[id][0] + W / 2, POS[id][1] + H]

  const running = (n: TopoNode) => n.status === 'ok'
  // kafka → sink 분기: kafka 오른쪽에서 나가 중간 x에서 위/아래로 꺾어 들어간다
  const kafkaR = right('kafka')
  const midX = 557

  return (
    <svg viewBox="0 0 940 392" className="h-full w-full">
      <style>{`
        .topo-flow { animation: topoFlow 0.9s linear infinite; }
        @keyframes topoFlow { to { stroke-dashoffset: -12; } }
      `}</style>

      {/* 본류 */}
      <EdgePath d={ortho([right('srcDb'), left('source')])} active={running(data.source)} />
      <EdgePath d={ortho([right('source'), left('kafka')])} active={running(data.source)} />
      <EdgePath
        d={ortho([kafkaR, [midX, kafkaR[1]], [midX, POS.jdbcSink[1] + H / 2], left('jdbcSink')])}
        active={running(data.jdbcSink)} />
      <EdgePath d={ortho([right('jdbcSink'), left('targetDb')])} active={running(data.jdbcSink)} />
      <EdgePath
        d={ortho([kafkaR, [midX, kafkaR[1]], [midX, POS.icebergSink[1] + H / 2], left('icebergSink')])}
        active={running(data.icebergSink)} />
      <EdgePath d={ortho([right('icebergSink'), left('iceberg')])} active={running(data.icebergSink)} />

      {/* 복구 레인 (평시 점선): Iceberg → recovery-job → Kafka */}
      <EdgePath dashed active={false}
        d={ortho([bottom('iceberg'), [POS.iceberg[0] + W / 2, 339], [POS.recovery[0] + W, 339]])} />
      <EdgePath dashed active={false}
        d={ortho([left('recovery'), [465, POS.recovery[1] + H / 2], [465, POS.kafka[1] + H]])} />

      {(Object.keys(POS) as (keyof typeof POS)[]).map((id) => (
        <Node key={id} id={id} n={data[id]} />
      ))}

      {/* 레인 주석 */}
      <text x={772} y={20} fontSize="9" fill="var(--chart-dim)">실 적재 (현재 상태)</text>
      <text x={772} y={206} fontSize="9" fill="var(--chart-dim)">changelog (복구 원본)</text>
      <text x={577} y={302} fontSize="9" fill="var(--chart-dim)">복구 재발행 (평시 정지)</text>
    </svg>
  )
}

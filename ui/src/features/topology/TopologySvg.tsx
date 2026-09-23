/**
 * 파일명 : TopologySvg.tsx
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 파이프라인 토폴로지 — 자체 SVG (React Flow 대체).
 * 소스·타깃 커넥션 개수만큼 노드를 동적으로 배치한다: 좌측 열 = 소스 커넥션별 노드,
 * 중앙 = Kafka 1개, 우측 상단 열 = 타깃 커넥션별 노드, 우측 하단 = Iceberg/MinIO 1개 +
 * recovery-job(점선, 평시 정지). 직교 라우팅으로 교차를 없앴고, 팬/줌은 라이브러리 없이
 * <g transform="translate scale">로 직접 구현한다(뷰포트 상태는 이 컴포넌트 로컬).
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성 (React Flow 제거)
 * --------------------------------------------------
 * 26. 08. 27.       | 최남희  | 하드코딩 hex를 CSS 변수(var(--ok) 등)로 교체 — 라이트 테마 대응
 * 26. 08. 27.       | 최남희  | 토큰명 accent-cyan → brand (VS Code풍 중립 팔레트 전환으로 시안 아님)
 * --------------------------------------------------
 * 26. 09. 22.       | 최남희  | 고정 8노드 레인 배치 → 소스·타깃 커넥션 개수만큼 동적 노드로
 * |                          | 전면 재작성 + 팬/줌 + 노드 클릭. 소스 2개 이상이면 한 노드에
 * |                          | 접합돼 sub 텍스트가 잘렸고, 타깃은 항상 1개만(find 첫 값) 그려
 * |                          | 실제 배선(소스 3개·타깃 3개)과 다른 그림이 됐다 — 다중 소스·
 * |                          | 다중 타깃이 실사용 단계에 들어가 더는 "범위 밖"으로 둘 수 없어
 * |                          | 26.09.07 판단(개별 노드는 범위 밖, 접두 집계로 요약)을 뒤집는다.
 * |                          | 소스DB+dz-source, 타깃DB+jdbc-sink는 1:1이라 노드는 그대로 합친다.
 * |                          | (보완) 팬/줌 버튼이 타깃 열 첫 노드와 겹쳐 그림 위 absolute 배치를
 * |                          | 버리고 SVG 위 별도 툴바 행으로 이동. 노드 폭 200→240, sub에서 DB
 * |                          | 타입을 빼 host:port/db만 남기고(그래도 넘치면 clipPath+title) 타입은
 * |                          | 라벨 줄 오른쪽 끝 작은 태그로 — sub가 여전히 잘린다는 지적 반영
 * |                          | (보완2) 노드 안 sub·meta·타입 태그·레인 주석이 흐려 안 보인다는
 * |                          | 지적 — 색을 chart-dim(ink-3)에서 ink-2로, 글자 크기 1px씩 올림
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | typeTag 옆에 DB 벤더 글리프(DbVendorLogo, simple-icons) 추가 —
 * |                          | PostgreSQL만 아이콘 있음(simple-icons에 Oracle 미등재), Oracle은
 * |                          | 텍스트만 유지
 * --------------------------------------------------
 */
import { useId, useRef, useState } from 'react'
import { DbVendorLogo, hasVendorLogo } from '@/components/DbVendorLogo'

export type NodeStatus = 'ok' | 'warn' | 'crit' | 'none'

export interface TopoNode {
  /** 소스/타깃 노드만 고유 id(`source:<connectionId>` 등) — 클릭 시 onNodeClick에 전달 */
  id: string
  label: string
  sub: string
  /** 라벨 줄 오른쪽 끝의 작은 태그 — 예: "ORACLE"/"POSTGRESQL". sub에서 DB 타입을 빼는 대신 여기 둔다 */
  typeTag?: string
  /** 노드 안 3번째 줄(작은 글씨) — 예: "dz-source-orcl225 · 5개 테이블", "jdbc-sink 3개" */
  meta?: string
  status: NodeStatus
  clickable?: boolean
}

export interface TopoData {
  sources: TopoNode[]
  kafka: TopoNode
  targets: TopoNode[]
  iceberg: TopoNode
  recovery: TopoNode
}

const STATUS_COLOR: Record<NodeStatus, string> = {
  ok: 'var(--ok)',
  warn: 'var(--warn)',
  crit: 'var(--crit)',
  none: 'var(--chart-dim)',
}

// --- 레이아웃 상수 (viewBox는 소스·타깃 개수에 맞춰 동적 계산) ---
const NW = 240
const NH = 64
const PAD = 24
const VGAP = 18 // 같은 열에서 노드 사이 간격
const COLGAP = 170 // 열과 열 사이 간격(간선이 꺾이는 공간 포함)
const SECTION_GAP = 40 // 타깃 블록 → Iceberg → recovery 사이 간격

interface PositionedNode {
  node: TopoNode
  x: number
  y: number
}

interface Layout {
  width: number
  height: number
  sources: PositionedNode[]
  kafka: PositionedNode
  targets: PositionedNode[]
  iceberg: PositionedNode
  recovery: PositionedNode
}

function computeLayout(data: TopoData): Layout {
  const n = Math.max(data.sources.length, 1)
  const m = Math.max(data.targets.length, 1)

  const sourcesX = PAD
  const kafkaX = sourcesX + NW + COLGAP
  const rightX = kafkaX + NW + COLGAP

  const leftBlockH = n * NH + (n - 1) * VGAP
  const targetsBlockH = m * NH + (m - 1) * VGAP
  const rightBlockH = targetsBlockH + SECTION_GAP + NH + SECTION_GAP + NH // 타깃 + iceberg + recovery

  const contentH = Math.max(leftBlockH, rightBlockH, NH)
  const height = PAD * 2 + contentH
  const width = rightX + NW + PAD

  const leftTop = PAD + (contentH - leftBlockH) / 2
  const sources = data.sources.map((node, i) => ({ node, x: sourcesX, y: leftTop + i * (NH + VGAP) }))

  const rightTop = PAD + (contentH - rightBlockH) / 2
  const targets = data.targets.map((node, j) => ({ node, x: rightX, y: rightTop + j * (NH + VGAP) }))
  const icebergY = rightTop + targetsBlockH + SECTION_GAP
  const recoveryY = icebergY + NH + SECTION_GAP

  const kafkaY = PAD + contentH / 2 - NH / 2

  return {
    width,
    height,
    sources,
    kafka: { node: data.kafka, x: kafkaX, y: kafkaY },
    targets,
    iceberg: { node: data.iceberg, x: rightX, y: icebergY },
    recovery: { node: data.recovery, x: rightX, y: recoveryY },
  }
}

const right = (p: PositionedNode): [number, number] => [p.x + NW, p.y + NH / 2]
const left = (p: PositionedNode): [number, number] => [p.x, p.y + NH / 2]
const top = (p: PositionedNode): [number, number] => [p.x + NW / 2, p.y]
const bottom = (p: PositionedNode): [number, number] => [p.x + NW / 2, p.y + NH]

/** 직교 경로 — 점 목록을 꺾인선으로 */
function ortho(points: [number, number][]): string {
  return points.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x},${y}`).join(' ')
}

function EdgePath({ d, active, dashed }: { d: string; active: boolean; dashed?: boolean }) {
  return (
    <path
      d={d}
      fill="none"
      stroke={active ? 'var(--brand)' : 'var(--chart-grid)'}
      strokeWidth="2"
      strokeDasharray={dashed ? '5 5' : active ? '7 5' : undefined}
      className={active && !dashed ? 'topo-flow' : undefined}
    />
  )
}

function Node({
  p,
  clipId,
  onClick,
}: {
  p: PositionedNode
  clipId: string
  onClick?: (rect: DOMRect) => void
}) {
  const { node: n, x, y } = p
  const clickable = !!n.clickable && !!onClick
  const innerLeft = x + 16
  const textW = NW - 32
  // 라벨 줄 오른쪽에 typeTag(ORACLE 등) 자리를 남겨둔다 — typeTag 없으면 라벨이 전체 폭을 쓴다.
  // 벤더 글리프가 있는 타입(PostgreSQL)은 아이콘+간격만큼 조금 더 넓게 예약한다.
  const typeTagW = n.typeTag ? (hasVendorLogo(n.typeTag) ? 92 : 78) : 0
  const labelW = NW - 30 - 10 - typeTagW

  return (
    <g
      onClick={
        clickable
          ? (e) => onClick!((e.currentTarget as SVGGElement).getBoundingClientRect())
          : undefined
      }
      style={clickable ? { cursor: 'pointer' } : undefined}
      className={clickable ? 'topo-node-clickable' : undefined}
    >
      <title>{`${n.label}${n.typeTag ? ' · ' + n.typeTag : ''}\n${n.sub}${n.meta ? '\n' + n.meta : ''}`}</title>
      <defs>
        <clipPath id={`${clipId}-label`}><rect x={x + 30} y={y + 9} width={labelW} height={16} /></clipPath>
        <clipPath id={`${clipId}-sub`}><rect x={innerLeft} y={y + 30} width={textW} height={13} /></clipPath>
        <clipPath id={`${clipId}-meta`}><rect x={innerLeft} y={y + 44} width={textW} height={13} /></clipPath>
      </defs>
      <rect x={x} y={y} width={NW} height={NH} rx={10}
        fill="var(--surface2)" stroke="var(--chart-grid)" strokeWidth="1" />
      <circle cx={x + 16} cy={y + 19} r={4} fill={STATUS_COLOR[n.status]}>
        {n.status === 'crit' && (
          <animate attributeName="opacity" values="1;0.25;1" dur="1.2s" repeatCount="indefinite" />
        )}
      </circle>
      <g clipPath={`url(#${clipId}-label)`}>
        <text x={x + 30} y={y + 22} fontSize="12.5" fontWeight="600" fill="var(--foreground)">{n.label}</text>
      </g>
      {n.typeTag && (
        <>
          <DbVendorLogo dbType={n.typeTag} x={x + NW - typeTagW + 2} y={y + 11} />
          <text x={x + NW - 14} y={y + 21} fontSize="9" fontWeight="600" textAnchor="end" fill="var(--ink-2)">
            {n.typeTag}
          </text>
        </>
      )}
      <g clipPath={`url(#${clipId}-sub)`}>
        <text x={innerLeft} y={y + 40} fontSize="10.5" fontFamily="monospace" fill="var(--ink-2)">{n.sub}</text>
      </g>
      {n.meta && (
        <g clipPath={`url(#${clipId}-meta)`}>
          <text x={innerLeft} y={y + 54} fontSize="9.5" fontFamily="monospace" fill="var(--ink-2)">{n.meta}</text>
        </g>
      )}
    </g>
  )
}

interface ViewState { tx: number; ty: number; scale: number }
const INITIAL_VIEW: ViewState = { tx: 0, ty: 0, scale: 1 }
const SCALE_MIN = 0.5
const SCALE_MAX = 2.5

export function TopologySvg({
  data,
  onNodeClick,
}: {
  data: TopoData
  /** 소스/타깃 노드 클릭 — 카드 배치용 뷰포트 좌표(DOMRect)를 함께 전달 */
  onNodeClick?: (id: string, rect: DOMRect) => void
}) {
  const uid = useId()
  const svgRef = useRef<SVGSVGElement>(null)
  const [view, setView] = useState<ViewState>(INITIAL_VIEW)
  const [dragging, setDragging] = useState(false)
  // down: 포인터가 눌린 상태 · dragging: 임계값을 넘어 실제 팬으로 전환됨(이때만 pointer capture) —
  // click 이벤트의 대상이 svg로 가로채이지 않도록, capture는 이동이 확인된 뒤에만 건다
  const dragRef = useRef<{ down: boolean; dragging: boolean; moved: boolean; startX: number; startY: number; tx: number; ty: number; pointerId: number }>({
    down: false, dragging: false, moved: false, startX: 0, startY: 0, tx: 0, ty: 0, pointerId: 0,
  })

  const layout = computeLayout(data)
  const vb = { x: 0, y: 0, w: layout.width, h: layout.height }

  const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v))

  /** 렌더 스케일(px→viewBox) — preserveAspectRatio(xMidYMid meet) 기준 */
  const renderScale = () => {
    const svg = svgRef.current
    if (!svg) return { scale: 1, offsetX: 0, offsetY: 0, rect: svg as unknown as DOMRect }
    const rect = svg.getBoundingClientRect()
    const scale = Math.min(rect.width / vb.w, rect.height / vb.h)
    const offsetX = (rect.width - vb.w * scale) / 2
    const offsetY = (rect.height - vb.h * scale) / 2
    return { scale, offsetX, offsetY, rect }
  }

  const zoomAt = (clientX: number, clientY: number, factor: number) => {
    const { scale, offsetX, offsetY, rect } = renderScale()
    if (!rect) return
    const mx = vb.x + (clientX - rect.left - offsetX) / scale
    const my = vb.y + (clientY - rect.top - offsetY) / scale
    setView((v) => {
      const next = clamp(v.scale * factor, SCALE_MIN, SCALE_MAX)
      const plx = (mx - v.tx) / v.scale
      const ply = (my - v.ty) / v.scale
      return { scale: next, tx: mx - next * plx, ty: my - next * ply }
    })
  }

  const zoomAtCenter = (factor: number) => {
    const svg = svgRef.current
    if (!svg) return
    const rect = svg.getBoundingClientRect()
    zoomAt(rect.left + rect.width / 2, rect.top + rect.height / 2, factor)
  }

  const onWheel = (e: React.WheelEvent<SVGSVGElement>) => {
    if (!e.ctrlKey) return // 일반 휠은 페이지 스크롤 그대로
    e.preventDefault()
    zoomAt(e.clientX, e.clientY, e.deltaY < 0 ? 1.12 : 1 / 1.12)
  }

  const onPointerDown = (e: React.PointerEvent<SVGSVGElement>) => {
    if (e.button !== 0) return
    dragRef.current = { down: true, dragging: false, moved: false, startX: e.clientX, startY: e.clientY, tx: view.tx, ty: view.ty, pointerId: e.pointerId }
  }
  const onPointerMove = (e: React.PointerEvent<SVGSVGElement>) => {
    const d = dragRef.current
    if (!d.down) return
    const dxPx = e.clientX - d.startX
    const dyPx = e.clientY - d.startY
    if (!d.dragging) {
      if (Math.abs(dxPx) + Math.abs(dyPx) <= 3) return
      d.dragging = true
      d.moved = true
      setDragging(true)
      ;(e.currentTarget as SVGSVGElement).setPointerCapture(d.pointerId)
    }
    const { scale } = renderScale()
    setView((v) => ({ ...v, tx: d.tx + dxPx / scale, ty: d.ty + dyPx / scale }))
  }
  const onPointerUp = (e: React.PointerEvent<SVGSVGElement>) => {
    const d = dragRef.current
    if (d.dragging) {
      try { (e.currentTarget as SVGSVGElement).releasePointerCapture(d.pointerId) } catch { /* 이미 해제됐으면 무시 */ }
    }
    d.down = false
    d.dragging = false
    setDragging(false)
    // click 이벤트(드래그 직후 노드 클릭 오인 방지)가 moved를 먼저 읽도록 다음 tick에 리셋
    setTimeout(() => { dragRef.current.moved = false }, 0)
  }

  const handleNodeClick = (id: string, rect: DOMRect) => {
    if (dragRef.current.moved) return
    onNodeClick?.(id, rect)
  }

  const kafkaR = right(layout.kafka)
  const midX1 = PAD + NW + COLGAP / 2
  const midX2 = layout.kafka.x + NW + COLGAP / 2
  const midX3 = layout.kafka.x + NW + COLGAP * 0.35

  const edges: { d: string; active: boolean; dashed?: boolean }[] = []
  for (const s of layout.sources) {
    edges.push({ d: ortho([right(s), [midX1, right(s)[1]], [midX1, left(layout.kafka)[1]], left(layout.kafka)]), active: s.node.status === 'ok' })
  }
  for (const t of layout.targets) {
    edges.push({ d: ortho([kafkaR, [midX2, kafkaR[1]], [midX2, left(t)[1]], left(t)]), active: t.node.status === 'ok' })
  }
  edges.push({ d: ortho([kafkaR, [midX2, kafkaR[1]], [midX2, layout.iceberg.y + NH / 2], left(layout.iceberg)]), active: layout.iceberg.node.status === 'ok' })

  return (
    <div className="flex h-full w-full flex-col">
      {/* 팬/줌 툴바 — 그림 위에 겹치지 않는 별도 행 */}
      <div className="flex shrink-0 justify-end gap-1 pb-1.5">
        <button type="button" title="확대"
          className="flex size-6 items-center justify-center rounded border border-line-2 bg-card text-xs font-semibold text-ink-2 hover:border-primary hover:text-primary"
          onClick={() => zoomAtCenter(1.25)}>+</button>
        <button type="button" title="축소"
          className="flex size-6 items-center justify-center rounded border border-line-2 bg-card text-xs font-semibold text-ink-2 hover:border-primary hover:text-primary"
          onClick={() => zoomAtCenter(1 / 1.25)}>−</button>
        <button type="button" title="맞춤"
          className="flex h-6 items-center justify-center rounded border border-line-2 bg-card px-1.5 text-[9px] font-semibold text-ink-2 hover:border-primary hover:text-primary"
          onClick={() => setView(INITIAL_VIEW)}>맞춤</button>
      </div>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${vb.w} ${vb.h}`}
        className="min-h-0 flex-1 w-full touch-none"
        style={{ cursor: dragging ? 'grabbing' : 'grab' }}
        onWheel={onWheel}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerLeave={onPointerUp}
      >
        <style>{`
          .topo-flow { animation: topoFlow 0.9s linear infinite; }
          @keyframes topoFlow { to { stroke-dashoffset: -12; } }
          .topo-node-clickable:hover rect:first-of-type { stroke: var(--brand); }
        `}</style>
        <g transform={`translate(${view.tx} ${view.ty}) scale(${view.scale})`}>
          {edges.map((e, i) => <EdgePath key={i} d={e.d} active={e.active} />)}

          {/* 복구 레인 (평시 점선): Iceberg → recovery-job → Kafka */}
          <EdgePath dashed active={false} d={ortho([bottom(layout.iceberg), top(layout.recovery)])} />
          <EdgePath dashed active={false}
            d={ortho([left(layout.recovery), [midX3, left(layout.recovery)[1]], [midX3, bottom(layout.kafka)[1]], bottom(layout.kafka)])} />

          {layout.sources.map((p, i) => (
            <Node key={p.node.id} p={p} clipId={`${uid}-src${i}`} onClick={(rect) => handleNodeClick(p.node.id, rect)} />
          ))}
          <Node p={layout.kafka} clipId={`${uid}-kafka`} />
          {layout.targets.map((p, j) => (
            <Node key={p.node.id} p={p} clipId={`${uid}-tgt${j}`} onClick={(rect) => handleNodeClick(p.node.id, rect)} />
          ))}
          <Node p={layout.iceberg} clipId={`${uid}-ice`} />
          <Node p={layout.recovery} clipId={`${uid}-rec`} />

          {/* 레인 주석 */}
          {layout.targets[0] && (
            <text x={layout.targets[0].x + 2} y={layout.targets[0].y - 8} fontSize="9.5" fill="var(--ink-2)">실 적재 (현재 상태)</text>
          )}
          <text x={layout.iceberg.x + 2} y={layout.iceberg.y - 8} fontSize="9.5" fill="var(--ink-2)">changelog (복구 원본)</text>
          <text x={layout.recovery.x + 2} y={layout.recovery.y - 8} fontSize="9.5" fill="var(--ink-2)">복구 재발행 (평시 정지)</text>
        </g>
      </svg>
    </div>
  )
}

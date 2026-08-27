/**
 * 파일명 : LineChart.tsx
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 대시보드용 경량 시계열 라인 차트 (SVG 직접 — 라이브러리 없음).
 * 2px 라인·은은한 그리드·범례+계열 끝 직접 라벨·크로스헤어 툴팁.
 * 계열 색은 다크 서피스 기준 CVD 검증을 통과한 고정 팔레트를 쓴다
 * (--chart-series-1 시안, --chart-series-2 바이올렛 — 순서 고정, 상태색(ok/warn/crit)과 분리.
 * 라이트 테마 값은 index.css :root 참조).
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | timeFormat 프롭 — 시간/일 해상도의 축·툴팁 표기 지원
 * --------------------------------------------------
 * 26. 08. 27.       | 최남희  | 하드코딩 hex를 CSS 변수로 교체 — 라이트 테마 대응
 * --------------------------------------------------
 */
import { useMemo, useRef, useState } from 'react'

export const CHART_SERIES_COLORS = ['var(--chart-series-1)', 'var(--chart-series-2)'] as const

export interface ChartSeries {
  name: string
  color: string
  points: { ts: number; value: number }[]
}

const PAD = { left: 44, right: 76, top: 10, bottom: 22 }

export function LineChart({
  series,
  height = 180,
  format = (v: number) => v.toLocaleString(),
  timeFormat,
}: {
  series: ChartSeries[]
  height?: number
  format?: (v: number) => string
  /** x축·툴팁 시각 표기 (기본 HH:MM — 일 단위 등 긴 주기는 호출측이 지정) */
  timeFormat?: (ts: number) => string
}) {
  const ref = useRef<HTMLDivElement>(null)
  const [width, setWidth] = useState(560)
  const [hoverTs, setHoverTs] = useState<number | null>(null)

  // 컨테이너 폭 추적 (ResizeObserver — 렌더 후 1회 + 리사이즈)
  useMemo(() => {
    if (typeof ResizeObserver === 'undefined') return
    const ro = new ResizeObserver((es) => {
      for (const e of es) setWidth(Math.max(320, e.contentRect.width))
    })
    setTimeout(() => ref.current && ro.observe(ref.current), 0)
  }, [])

  const all = series.flatMap((s) => s.points)
  const w = width
  const h = height
  const iw = w - PAD.left - PAD.right
  const ih = h - PAD.top - PAD.bottom

  if (all.length === 0) {
    return (
      <div ref={ref} className="flex items-center justify-center text-xs text-muted-foreground"
        style={{ height }}>
        수집된 샘플이 아직 없습니다 (1분 주기 수집)
      </div>
    )
  }

  const t0 = Math.min(...all.map((p) => p.ts))
  const t1 = Math.max(...all.map((p) => p.ts))
  const vMax = Math.max(1, ...all.map((p) => p.value))
  const x = (ts: number) => PAD.left + (t1 === t0 ? iw / 2 : ((ts - t0) / (t1 - t0)) * iw)
  const y = (v: number) => PAD.top + ih - (v / vMax) * ih

  const path = (pts: { ts: number; value: number }[]) =>
    pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(p.ts).toFixed(1)},${y(p.value).toFixed(1)}`).join(' ')

  const timeLabel = timeFormat ?? ((ts: number) => {
    const d = new Date(ts)
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
  })

  // 크로스헤어: 가장 가까운 시점의 각 계열 값
  const hover = hoverTs === null ? null : (() => {
    const tss = [...new Set(all.map((p) => p.ts))].sort((a, b) => a - b)
    let nearest = tss[0]
    for (const t of tss) if (Math.abs(t - hoverTs) < Math.abs(nearest - hoverTs)) nearest = t
    return {
      ts: nearest,
      values: series.map((s) => ({
        name: s.name, color: s.color,
        value: s.points.find((p) => p.ts === nearest)?.value ?? null,
      })),
    }
  })()

  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect()
    const px = e.clientX - rect.left
    if (px < PAD.left || px > w - PAD.right) return setHoverTs(null)
    setHoverTs(t0 + ((px - PAD.left) / iw) * (t1 - t0))
  }

  const gridY = [0.5, 1].map((f) => vMax * f)

  return (
    <div ref={ref} className="relative">
      <div className="mb-1 flex gap-4 text-[11px] text-muted-foreground">
        {series.map((s) => (
          <span key={s.name} className="flex items-center gap-1.5">
            <span className="inline-block h-[3px] w-4 rounded" style={{ background: s.color }} />
            {s.name}
          </span>
        ))}
      </div>
      <svg width={w} height={h} onMouseMove={onMove} onMouseLeave={() => setHoverTs(null)}>
        {gridY.map((v) => (
          <g key={v}>
            <line x1={PAD.left} x2={w - PAD.right} y1={y(v)} y2={y(v)}
              stroke="var(--chart-grid)" strokeWidth="1" />
            <text x={PAD.left - 6} y={y(v) + 3} textAnchor="end"
              className="fill-muted-foreground" fontSize="10" fontFamily="monospace">
              {format(v)}
            </text>
          </g>
        ))}
        <line x1={PAD.left} x2={w - PAD.right} y1={y(0)} y2={y(0)} stroke="var(--chart-grid)" />
        {[t0, (t0 + t1) / 2, t1].map((t, i) => (
          <text key={i} x={x(t)} y={h - 6} textAnchor="middle"
            className="fill-muted-foreground" fontSize="10" fontFamily="monospace">
            {timeLabel(t)}
          </text>
        ))}
        {series.map((s) => (
          <path key={s.name} d={path(s.points)} fill="none" stroke={s.color} strokeWidth="2" />
        ))}
        {/* 계열 끝 직접 라벨 — 색만으로 구분하지 않는다. 값이 겹치면 아래로 밀어 충돌 회피 */}
        {(() => {
          const placed: number[] = []
          return series.map((s) => {
            const last = s.points[s.points.length - 1]
            if (!last) return null
            let ly = Math.min(y(last.value) + 3, PAD.top + ih - 14) // x축 눈금과 겹침 방지
            while (placed.some((p) => Math.abs(p - ly) < 11)) ly -= 12
            placed.push(ly)
            return (
              <text key={s.name} x={x(last.ts) + 6} y={ly}
                className="fill-muted-foreground" fontSize="10">
                {s.name}
              </text>
            )
          })
        })()}
        {hover && (
          <g>
            <line x1={x(hover.ts)} x2={x(hover.ts)} y1={PAD.top} y2={PAD.top + ih}
              stroke="var(--chart-dim)" strokeWidth="1" strokeDasharray="3 3" />
            {hover.values.map((v) =>
              v.value !== null ? (
                <circle key={v.name} cx={x(hover.ts)} cy={y(v.value)} r="3.5"
                  fill={v.color} stroke="var(--card)" strokeWidth="2" />
              ) : null)}
          </g>
        )}
      </svg>
      {hover && (
        <div className="pointer-events-none absolute rounded border border-border bg-card px-2 py-1 font-mono text-[10px] shadow"
          style={{
            left: Math.min(x(hover.ts) + 10, w - 150),
            top: 18,
          }}>
          <div className="text-muted-foreground">{timeLabel(hover.ts)}</div>
          {hover.values.map((v) => (
            <div key={v.name} className="flex items-center gap-1.5">
              <span className="inline-block h-2 w-2 rounded-full" style={{ background: v.color }} />
              <span className="text-muted-foreground">{v.name}</span>
              <span className="ml-auto pl-2 text-foreground">
                {v.value === null ? '—' : format(v.value)}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

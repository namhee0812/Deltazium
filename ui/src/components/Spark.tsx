/** 스파크라인 — ui-reference 프로토타입의 Spark 컴포넌트 이식 */
export function Spark({
  data,
  color,
  w = 90,
  h = 20,
}: {
  data: number[]
  color: string
  w?: number
  h?: number
}) {
  const max = Math.max(...data, 1)
  const pts = data
    .map((v, i) => `${(i / (data.length - 1)) * w},${h - (v / max) * (h - 3) - 1}`)
    .join(' ')
  return (
    <svg width={w} height={h} className="block">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" />
    </svg>
  )
}

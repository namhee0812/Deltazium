/**
 * 파일명 : Spark.tsx
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : 스파크라인 — ui-reference 프로토타입의 Spark 컴포넌트 이식
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
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

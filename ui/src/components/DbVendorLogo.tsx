/**
 * 파일명 : DbVendorLogo.tsx
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : DB 벤더 단색 글리프 — 토폴로지 노드(TopologySvg)·노드 정보 카드(TopologyPanel)의
 * "ORACLE"/"POSTGRESQL" 텍스트 태그 옆에 붙인다. simple-icons(CC0) path 데이터를 SVG
 * <path>로 인라인 렌더한다(외부 요청 없음). 브랜드 컬러 대신 텍스트와 같은 단색
 * (var(--ink-2))만 쓴다 — 다크/라이트 양쪽에서 읽혀야 하고 상태 점 색과 충돌하지 않아야
 * 하기 때문(UI 최소주의: 벤더 색은 사용자 판단에 영향을 주지 않는 장식).
 *
 * simple-icons에는 Oracle 글리프가 없다(상표 요청으로 제거됨, 2026-09-23 확인 —
 * `siOracle` export 없음). Oracle은 자체 링 글리프(단순 기하 도형, 상표 아님)를 쓴다.
 * 빨간색 등 브랜드 색을 원하면 아래 <path>의 fill(`var(--ink-2)`)을 벤더별로 분기하는
 * 한 줄만 바꾸면 된다(VENDOR_ICON에 color 필드를 추가하는 방식).
 * <svg>를 중첩 가능한 요소로 만들어 TopologySvg(부모가 이미 <svg>)에선 x/y로,
 * TopologyPanel(일반 HTML 흐름)에선 그냥 인라인으로 쓴다.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | Databricks 글리프 추가(simple-icons siDatabricks)
 * --------------------------------------------------
 * 26. 10. 26.       | 최남희  | Oracle 자체 링 글리프 추가 — simple-icons 미등재라 텍스트만
 * |                          | 있던 ORACLE이 hasVendorLogo true가 돼 노드 라벨 폭 예약도 맞춰진다
 * --------------------------------------------------
 */
import { siDatabricks, siPostgresql } from 'simple-icons/icons'

const VENDOR_ICON: Partial<Record<string, { path: string; title: string }>> = {
  POSTGRESQL: siPostgresql,
  DATABRICKS: siDatabricks,
  // simple-icons 미등재 — 직접 그린 타원 링(evenodd로 안쪽을 비움, 24x24 기준)
  ORACLE: {
    title: 'Oracle',
    path: 'M12 4.5C6.5 4.5 2 7.9 2 12s4.5 7.5 10 7.5 10-3.4 10-7.5-4.5-7.5-10-7.5Zm0 4c-2.9 0-5.2 1.6-5.2 3.5s2.3 3.5 5.2 3.5 5.2-1.6 5.2-3.5S14.9 8.5 12 8.5Z',
  },
}

/** dbType(예: "ORACLE"/"POSTGRESQL")에 등재된 글리프가 있는지 — 레이아웃 폭 계산용 */
export function hasVendorLogo(dbType: string): boolean {
  return dbType in VENDOR_ICON
}

export function DbVendorLogo({
  dbType,
  size = 13,
  x,
  y,
}: {
  dbType: string
  size?: number
  /** SVG 부모(TopologySvg) 안에 배치할 때만 지정 — 중첩 <svg>의 x/y */
  x?: number
  y?: number
}) {
  const icon = VENDOR_ICON[dbType]
  if (!icon) return null
  return (
    <svg
      x={x}
      y={y}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="var(--ink-2)"
      fillRule="evenodd"
      aria-hidden="true"
    >
      <path d={icon.path} />
    </svg>
  )
}

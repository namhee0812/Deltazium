/**
 * 파일명 : connect.ts
 * 작성일자 : 26. 08. 04.
 * 작성자 : 최남희
 * 설명 : Kafka Connect 커넥터 상태 공용 타입·판정 헬퍼.
 * Connect는 connector 상태와 task 상태가 별개다 — connector가 RUNNING이어도
 * task는 FAILED일 수 있으므로(가장 흔한 장애 형태), 화면 표시는 반드시
 * 둘을 합친 최악값(effectiveState)을 쓴다.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 최초 생성 (task FAILED가 UI에 안 보이던 문제 수정)
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 캡처 배너용 traceOf·causeLine(Caused by 추출) 헬퍼 추가
 * --------------------------------------------------
 */

export interface ConnectorInfo {
  status?: {
    connector?: { state?: string }
    tasks?: { id?: number; state?: string; trace?: string }[]
  }
}

export type ConnectorStates = Record<string, ConnectorInfo>

/** connector·task 상태의 최악값. task가 하나라도 FAILED면 FAILED. */
export function effectiveState(info: ConnectorInfo | undefined): string {
  if (!info) return 'UNKNOWN'
  const conn = info.status?.connector?.state ?? 'UNKNOWN'
  const tasks = info.status?.tasks ?? []
  if (conn === 'FAILED' || tasks.some((t) => t.state === 'FAILED')) return 'FAILED'
  return conn
}

/** status의 스택트레이스 전문 (connector 또는 첫 task). */
export function traceOf(info: ConnectorInfo | undefined): string | null {
  const tasks = info?.status?.tasks ?? []
  return tasks.find((t) => t.trace)?.trace ?? null
}

/** trace에서 근본 원인 한 줄 — 마지막 "Caused by:" (없으면 첫 줄). */
export function causeLine(info: ConnectorInfo | undefined): string | null {
  const trace = traceOf(info)
  if (!trace) return null
  const causes = trace.split('\n').filter((l) => l.startsWith('Caused by: '))
  const line = causes.length > 0 ? causes[causes.length - 1].slice('Caused by: '.length) : trace.split('\n')[0]
  return line.trim()
}

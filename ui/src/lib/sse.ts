/**
 * 파일명 : sse.ts
 * 작성일자 : 26. 08. 13.
 * 작성자 : 최남희
 * 설명 : POST 기반 SSE 소비 헬퍼. EventSource는 POST 요청을 지원하지 않아, /api/chat
 * 응답을 fetch 스트리밍으로 직접 파싱한다 — `\n\n` 단위로 이벤트를 분리하고 `data:` 줄만
 * JSON.parse한다.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 13.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */

export type ChatEvent =
  | { type: 'tool'; name: string; input?: unknown }
  | { type: 'answer'; text: string }
  | { type: 'error'; message: string }
  | { type: 'done' }

export async function streamChat(
  question: string,
  onEvent: (ev: ChatEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  const res = await fetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ question }),
    signal,
  })

  if (!res.ok || !res.body) {
    const body = await res.text().catch(() => '')
    throw new Error(body || `HTTP ${res.status}`)
  }

  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })

    let sepIdx: number
    while ((sepIdx = buffer.indexOf('\n\n')) !== -1) {
      const rawEvent = buffer.slice(0, sepIdx)
      buffer = buffer.slice(sepIdx + 2)

      const dataLine = rawEvent.split('\n').find((line) => line.startsWith('data:'))
      if (!dataLine) continue
      const payload = dataLine.slice('data:'.length).trim()
      if (!payload) continue

      try {
        onEvent(JSON.parse(payload) as ChatEvent)
      } catch {
        // 끊긴 청크로 인해 온전치 않은 줄 — 무시하고 다음 이벤트로 계속
      }
    }
  }
}

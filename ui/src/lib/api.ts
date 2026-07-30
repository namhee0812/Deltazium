/** backend REST 호출 헬퍼 — 실패 시 서버 error 메시지를 Error로 던진다. */
export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!res.ok) {
    let message = `HTTP ${res.status}`
    try {
      const body = (await res.json()) as { error?: string }
      if (body.error) message = body.error
    } catch {
      // body가 JSON이 아니면 상태 코드만
    }
    throw new Error(message)
  }
  // void 엔드포인트(정지/재개 등)는 200 + 빈 본문 — JSON 파싱 시도하면 안 됨
  const text = await res.text()
  if (!text) return undefined as T
  return JSON.parse(text) as T
}

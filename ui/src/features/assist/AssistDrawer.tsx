/**
 * 파일명 : AssistDrawer.tsx
 * 작성일자 : 26. 08. 14.
 * 작성자 : 최남희
 * 설명 : AI 진단 채팅 — 우측 고정 drawer(440/720px, 마스크 없음·비차단). 질문을
 * /api/chat(SSE)로 보내고 도구 진행 상황·최종 답변을 표시한다. 대화 히스토리는 화면
 * 표시용일 뿐 서버에는 매번 단일 질문만 보낸다(서버는 턴마다 독립 — ChatService 참고).
 * App.tsx 최상위에 탭 조건부 밖에서 항상 마운트되므로(열림 상태는 App이 소유해 prop으로
 * 내려받고, 닫힘 시에도 이 컴포넌트 자체는 계속 마운트돼 있다), 열림/닫힘·폭 확장은
 * 표시 토글일 뿐 대화 상태(및 진행 중인 스트리밍)는 drawer를 닫아도 유지된다.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 14.       | 최남희  | 최초 생성 (구 AssistPanel.tsx의 상단 탭 채팅을 흡수해 플로팅 위젯으로 전환)
 * --------------------------------------------------
 * 26. 09. 02.       | 최남희  | 우하단 플로팅 FAB+패널 → 상단 바 아이콘 트리거 + 우측 고정
 * |                          | drawer(절대 위치, 440/720px)로 전환. 열림 상태를 App.tsx로
 * |                          | 옮겨 open/onClose prop으로 제어(FAB가 상단 바 버튼으로 이동했으므로).
 * |                          | 플로팅 상태의 테이블/복구 drawer 하단 액션 바 가림 문제 해결.
 * --------------------------------------------------
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import { Loader2, Maximize2, Minimize2, X } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import type { Components } from 'react-markdown'
import { streamChat } from '@/lib/sse'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'

interface Message {
  role: 'user' | 'assistant' | 'error'
  text: string
  tools?: string[]
}

/** 도구 호출을 사람이 읽을 수 있는 진행 문구로. 백엔드 도구 클래스명 기반 이름이므로
 * 여기서 이름이 바뀌면 이 매핑도 같이 바뀌어야 한다 (ChatService 참고). */
function describeTool(name: string, input: unknown): string {
  if (name === 'get_overview_tool') return '전체 상태 조회'
  if (name === 'search_logs_tool') {
    const i = (input ?? {}) as { source?: string; q?: string }
    const parts = [i.source, i.q ? `'${i.q}'` : undefined].filter(Boolean)
    return parts.length > 0 ? `로그 검색 (${parts.join(', ')})` : '로그 검색'
  }
  return name
}

const markdownComponents: Components = {
  h1: (p) => <h3 className="mt-3 mb-1.5 text-[15px] font-semibold first:mt-0" {...p} />,
  h2: (p) => <h3 className="mt-3 mb-1.5 text-[14px] font-semibold first:mt-0" {...p} />,
  h3: (p) => <h4 className="mt-2.5 mb-1 text-[13px] font-semibold first:mt-0" {...p} />,
  p: (p) => <p className="mb-2 leading-relaxed last:mb-0" {...p} />,
  ul: (p) => <ul className="mb-2 list-disc space-y-0.5 pl-5 last:mb-0" {...p} />,
  ol: (p) => <ol className="mb-2 list-decimal space-y-0.5 pl-5 last:mb-0" {...p} />,
  li: (p) => <li {...p} />,
  a: (p) => <a className="text-primary underline underline-offset-2" target="_blank" rel="noreferrer" {...p} />,
  code: ({ className, children, ...rest }) => {
    const inline = !className
    return inline ? (
      <code className="rounded bg-secondary px-1 py-0.5 font-mono text-[12px]" {...rest}>
        {children}
      </code>
    ) : (
      <code className="block font-mono text-[11.5px] leading-relaxed" {...rest}>
        {children}
      </code>
    )
  },
  pre: (p) => (
    <pre
      className="mb-2 overflow-x-auto rounded-lg border border-border bg-background p-3 last:mb-0"
      {...p}
    />
  ),
  table: (p) => (
    <div className="mb-2 overflow-x-auto last:mb-0">
      <table className="w-full border-collapse text-[12px]" {...p} />
    </div>
  ),
  thead: (p) => <thead className="[&_tr]:border-b [&_tr]:border-border" {...p} />,
  tbody: (p) => <tbody {...p} />,
  tr: (p) => <tr className="border-b border-border last:border-0" {...p} />,
  th: (p) => <th className="px-2 py-1 text-left align-middle font-medium text-foreground" {...p} />,
  td: (p) => <td className="px-2 py-1 align-middle" {...p} />,
}

function MessageBubble({ msg }: { msg: Message }) {
  if (msg.role === 'user') {
    return (
      <div className="justify-self-end max-w-[85%] rounded-xl bg-accent px-3.5 py-2.5 text-sm text-accent-foreground whitespace-pre-wrap">
        {msg.text}
      </div>
    )
  }
  if (msg.role === 'error') {
    return (
      <div className="justify-self-start max-w-[85%] rounded-xl border border-crit/30 bg-crit/10 px-3.5 py-2.5 text-sm text-crit">
        {msg.text}
      </div>
    )
  }
  return (
    <div className="justify-self-start max-w-[85%] rounded-xl bg-card px-3.5 py-2.5 text-sm">
      {msg.tools && msg.tools.length > 0 && (
        <div className="mb-1.5 font-mono text-[10.5px] text-muted-foreground">
          {msg.tools.join(' · ')}
        </div>
      )}
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={markdownComponents}>
        {msg.text}
      </ReactMarkdown>
    </div>
  )
}

interface AssistDrawerProps {
  open: boolean
  onClose: () => void
}

export function AssistDrawer({ open, onClose }: AssistDrawerProps) {
  const [expanded, setExpanded] = useState(false)
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [progress, setProgress] = useState<string | null>(null)
  const endRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (open) endRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, progress, open])

  const send = useCallback(() => {
    const question = input.trim()
    if (!question || busy) return

    setMessages((prev) => [...prev, { role: 'user', text: question }])
    setInput('')
    setBusy(true)
    setProgress('생각 중…')

    const toolsUsed: string[] = []

    streamChat(question, (ev) => {
      if (ev.type === 'tool') {
        const label = describeTool(ev.name, ev.input)
        toolsUsed.push(label)
        setProgress(label)
      } else if (ev.type === 'answer') {
        setMessages((prev) => [...prev, { role: 'assistant', text: ev.text, tools: [...toolsUsed] }])
      } else if (ev.type === 'error') {
        setMessages((prev) => [...prev, { role: 'error', text: ev.message }])
      } else if (ev.type === 'done') {
        setBusy(false)
        setProgress(null)
      }
    }).catch((e: Error) => {
      setMessages((prev) => [...prev, { role: 'error', text: e.message }])
      setBusy(false)
      setProgress(null)
    })
  }, [input, busy])

  if (!open) return null

  return (
    <div
      className={`absolute top-0 right-0 bottom-0 z-40 flex flex-col border-l border-border bg-background shadow-[-12px_0_32px_rgba(16,24,40,.12)] transition-[width] ${
        expanded ? 'w-[720px]' : 'w-[440px]'
      }`}
    >
      <div className="flex items-center gap-1 bg-rail px-5 py-3.5 text-rail-ink">
        <span className="text-[15px] font-semibold">AI 진단</span>
        <div className="ml-auto flex items-center gap-1">
          <button
            className="text-rail-ink-2 hover:text-rail-ink"
            onClick={() => setExpanded((v) => !v)}
            title={expanded ? '축소' : '확장'}
          >
            {expanded ? <Minimize2 className="size-4" /> : <Maximize2 className="size-4" />}
          </button>
          <button className="text-rail-ink-2 hover:text-rail-ink" onClick={onClose} title="닫기">
            <X className="size-4.5" />
          </button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        {messages.length === 0 && (
          <p className="text-sm text-muted-foreground">
            CDC 파이프라인 상태에 대해 물어보세요. 지난 대화는 기억하지 않으니 질문마다 필요한
            맥락을 함께 적어주세요.
          </p>
        )}
        <div className="grid gap-3">
          {messages.map((m, i) => (
            <MessageBubble key={i} msg={m} />
          ))}
          {busy && (
            <div className="justify-self-start flex max-w-[85%] items-center gap-2 rounded-xl bg-card px-3.5 py-2.5 text-sm text-muted-foreground">
              <Loader2 className="size-3.5 animate-spin" />
              {progress}
            </div>
          )}
        </div>
        <div ref={endRef} />
      </div>

      <div className="flex gap-2 border-t border-border bg-surface-2 p-3">
        <Input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault()
              send()
            }
          }}
          placeholder="예: 왜 CDC가 멈췄어? / dz-source 상태 어때?"
          disabled={busy}
          className="flex-1"
        />
        <Button onClick={send} disabled={busy || !input.trim()}>
          전송
        </Button>
      </div>
    </div>
  )
}

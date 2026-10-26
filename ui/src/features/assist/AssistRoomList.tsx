/**
 * 파일명 : AssistRoomList.tsx
 * 작성일자 : 26. 10. 26.
 * 작성자 : 최남희
 * 설명 : AI 진단 위젯의 대화방 목록 화면. 행 = 제목 · 마지막 활동 상대시각 · 메시지 수 · 삭제.
 * 가장 최근 방 하나만 밝은 톤(bg-card), 나머지는 한 단계 어두운 톤(bg-muted)으로 눌러
 * "지금 이어 갈 대화"를 시선이 먼저 잡게 한다. 삭제는 확인 없이 즉시(대화는 보관 주기로
 * 어차피 정리되는 휘발성 데이터).
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
import { Plus, Trash2 } from 'lucide-react'
import { relativeTime } from '@/features/system/useSystemWarnings'

export interface ChatRoomInfo {
  id: number
  title: string | null
  createdAt: string
  lastActiveAt: string
  messageCount: number
}

interface Props {
  rooms: ChatRoomInfo[]
  onOpen: (id: number) => void
  onNew: () => void
  onDelete: (id: number) => void
}

export function AssistRoomList({ rooms, onOpen, onNew, onDelete }: Props) {
  return (
    <div className="flex flex-col gap-2">
      <button
        onClick={onNew}
        className="flex items-center justify-center gap-1.5 rounded-lg border border-dashed border-border py-2 text-sm text-muted-foreground hover:border-primary hover:text-primary"
      >
        <Plus className="size-4" /> 새 대화
      </button>
      {rooms.map((r, i) => (
        <div
          key={r.id}
          role="button"
          tabIndex={0}
          onClick={() => onOpen(r.id)}
          onKeyDown={(e) => { if (e.key === 'Enter') onOpen(r.id) }}
          className={`group flex cursor-pointer items-center gap-3 rounded-lg border border-border px-3.5 py-2.5 text-sm hover:border-primary ${
            i === 0 ? 'bg-card text-foreground' : 'bg-muted text-muted-foreground'
          }`}
        >
          <div className="min-w-0 flex-1">
            <div className="truncate font-medium">{r.title ?? '새 대화'}</div>
            <div className="mt-0.5 font-mono text-[11px] opacity-80">
              {relativeTime(Date.parse(r.lastActiveAt))} · 메시지 {r.messageCount}
            </div>
          </div>
          <button
            title="삭제"
            aria-label="대화 삭제"
            className="shrink-0 rounded p-1 hover:text-crit"
            onClick={(e) => { e.stopPropagation(); onDelete(r.id) }}
          >
            <Trash2 className="size-4" />
          </button>
        </div>
      ))}
    </div>
  )
}

/**
 * 파일명 : AssistSettings.tsx
 * 작성일자 : 26. 10. 26.
 * 작성자 : 최남희
 * 설명 : AI 진단 위젯 설정 팝오버(⚙). 두 항목만 — 대화 보관 주기(서버 전역 설정,
 * PUT /api/chat/settings)와 글자 크기(브라우저 개인 설정, localStorage dz.assist.font).
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
import { useState } from 'react'
import { Settings } from 'lucide-react'
import { api } from '@/lib/api'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

export type FontSize = 'sm' | 'md' | 'lg'
export const FONT_CLASS: Record<FontSize, string> = { sm: 'text-xs', md: 'text-sm', lg: 'text-base' }
const FONT_LABEL: Record<FontSize, string> = { sm: '소', md: '중', lg: '대' }
const RETENTION_DAYS = [1, 3, 7, 30]

interface Props {
  font: FontSize
  onFont: (f: FontSize) => void
}

export function AssistSettings({ font, onFont }: Props) {
  const [days, setDays] = useState<number | null>(null)

  const load = (o: boolean) => {
    if (!o) return
    api<{ retentionDays: number }>('/api/chat/settings')
      .then((s) => setDays(s.retentionDays))
      .catch(() => { /* 조회 실패 시 값 없이 표시 */ })
  }
  const change = (v: string) => {
    const d = Number(v)
    setDays(d)
    api('/api/chat/settings', { method: 'PUT', body: JSON.stringify({ retentionDays: d }) }).catch(() => load(true))
  }

  return (
    <Popover onOpenChange={load}>
      <PopoverTrigger asChild>
        <button className="text-rail-ink-2 hover:text-rail-ink" title="설정" aria-label="설정">
          <Settings className="size-4" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-60 space-y-3 text-foreground">
        <div className="flex items-center justify-between gap-3">
          <span className="text-sm">대화 보관</span>
          <Select value={days === null ? undefined : String(days)} onValueChange={change}>
            <SelectTrigger size="sm" className="w-24"><SelectValue placeholder="…" /></SelectTrigger>
            <SelectContent>
              {RETENTION_DAYS.map((d) => <SelectItem key={d} value={String(d)}>{d}일</SelectItem>)}
            </SelectContent>
          </Select>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-sm">글자 크기</span>
          <div role="radiogroup" aria-label="글자 크기" className="flex overflow-hidden rounded-md border border-border">
            {(Object.keys(FONT_LABEL) as FontSize[]).map((f) => (
              <button
                key={f}
                role="radio"
                aria-checked={font === f}
                onClick={() => onFont(f)}
                className={`px-3 py-1 text-sm ${font === f ? 'bg-primary text-white' : 'hover:bg-muted'}`}
              >
                {FONT_LABEL[f]}
              </button>
            ))}
          </div>
        </div>
      </PopoverContent>
    </Popover>
  )
}

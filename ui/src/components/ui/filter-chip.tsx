import type { ReactNode } from "react"

import { cn } from "@/lib/utils"

/* 필터 칩 — 999px pill, 선택 시 brand 역전, mono 카운트 (Components.dc.html anatomy) */

export function FilterChip({
  active,
  onClick,
  children,
  count,
  disabled = false,
  className,
}: {
  active: boolean
  onClick?: () => void
  children: ReactNode
  count?: number | string
  disabled?: boolean
  className?: string
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className={cn(
        "inline-flex h-7 items-center gap-1.5 rounded-full border px-2.5 text-xs font-semibold transition-colors disabled:cursor-not-allowed disabled:opacity-45",
        active
          ? "border-primary bg-primary text-primary-foreground"
          : "border-line-2 bg-card text-ink-2 hover:border-primary hover:text-primary",
        className
      )}
    >
      {children}
      {count !== undefined && (
        <span
          className={cn(
            "font-mono text-[11px] font-medium",
            active ? "opacity-75" : "text-ink-3"
          )}
        >
          {count}
        </span>
      )}
    </button>
  )
}

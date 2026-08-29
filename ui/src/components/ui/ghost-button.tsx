import type { ButtonHTMLAttributes } from "react"

import { cn } from "@/lib/utils"

/* 고스트 버튼 — 28px, hover 시 brand 테두리+soft 배경 (Main/Components.dc.html .ghost) */

export function GhostButton({
  className,
  children,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      type="button"
      className={cn(
        "inline-flex h-7 shrink-0 items-center gap-1.5 whitespace-nowrap rounded-md border border-line-2 bg-card px-2.5 text-xs font-medium text-ink-2 transition-colors hover:border-primary hover:bg-brand-soft hover:text-primary disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
      {...props}
    >
      {children}
    </button>
  )
}

import type { ReactNode } from "react"

import { cn } from "@/lib/utils"

/* 상태 배지 — Components.dc.html anatomy: 20px 높이, 점 + 텍스트, 상태색+soft 배경 */

export type StatusPillVariant = "ok" | "warn" | "crit" | "stop" | "brand"

const VARIANT_CLASS: Record<StatusPillVariant, string> = {
  ok: "text-ok bg-ok-soft",
  warn: "text-warn bg-warn-soft",
  crit: "text-crit bg-crit-soft",
  stop: "text-stop bg-stop-soft",
  brand: "text-primary bg-brand-soft",
}

export function StatusPill({
  variant,
  children,
  className,
  dot = true,
}: {
  variant: StatusPillVariant
  children: ReactNode
  className?: string
  /** 점 표시 여부 — 진행률 등 자체 텍스트가 이미 상태를 표현하면 끈다 */
  dot?: boolean
}) {
  return (
    <span
      className={cn(
        "inline-flex h-5 shrink-0 items-center gap-1.5 whitespace-nowrap rounded-full px-2 text-[11px] font-semibold",
        VARIANT_CLASS[variant],
        className
      )}
    >
      {dot && <span className="size-1.5 shrink-0 rounded-full bg-current" />}
      {children}
    </span>
  )
}

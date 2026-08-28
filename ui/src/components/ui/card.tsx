import * as React from "react"

import { cn } from "@/lib/utils"

/* Deltazium 카드 anatomy (2026-08-28 리디자인, Components.dc.html 기준):
   Card = 12px radius·1px border·2단 그림자 컨테이너. 섹션(Header/Content/Footer)이
   각자 패딩·구분선을 갖는 구조로, 상위 shadcn의 --card-spacing gap 모델 대신 쓴다. */

function Card({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="card"
      className={cn(
        "flex flex-col overflow-hidden rounded-lg border border-border bg-card text-sm text-card-foreground shadow-[var(--shadow-card)]",
        className
      )}
      {...props}
    />
  )
}

function CardHeader({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="card-header"
      className={cn(
        "flex items-center gap-2.5 border-b border-border px-4 py-3",
        className
      )}
      {...props}
    />
  )
}

function CardTitle({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="card-title"
      className={cn("text-[13.5px] font-semibold leading-normal text-foreground", className)}
      {...props}
    />
  )
}

function CardDescription({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="card-description"
      className={cn("text-xs text-ink-3", className)}
      {...props}
    />
  )
}

function CardAction({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div data-slot="card-action" className={cn("ml-auto self-center", className)} {...props} />
  )
}

function CardContent({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div data-slot="card-content" className={cn("px-4 py-3", className)} {...props} />
  )
}

function CardFooter({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="card-footer"
      className={cn(
        "flex items-center gap-2 border-t border-border bg-surface-2 px-4 py-2.5",
        className
      )}
      {...props}
    />
  )
}

export { Card, CardHeader, CardFooter, CardTitle, CardAction, CardDescription, CardContent }

import { cn } from "@/lib/utils"

/* 기간 선택 세그먼트 컨트롤 (Main.dc.html .seg anatomy) */

export function Segmented<T extends string>({
  options,
  value,
  onChange,
  className,
}: {
  options: readonly { key: T; label: string; title?: string }[]
  value: T
  onChange: (key: T) => void
  className?: string
}) {
  return (
    <div className={cn("inline-flex overflow-hidden rounded-md border border-line-2", className)}>
      {options.map((o) => (
        <button
          key={o.key}
          type="button"
          title={o.title}
          onClick={() => onChange(o.key)}
          className={cn(
            "flex h-[26px] items-center px-2.5 text-xs transition-colors",
            value === o.key
              ? "bg-primary font-semibold text-primary-foreground"
              : "text-ink-2 hover:text-foreground"
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  )
}

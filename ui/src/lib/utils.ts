/**
 * 파일명 : utils.ts
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : Tailwind 클래스 병합 유틸(cn) — shadcn/ui 표준.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

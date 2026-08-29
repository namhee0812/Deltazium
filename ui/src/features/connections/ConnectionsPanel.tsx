/**
 * 파일명 : ConnectionsPanel.tsx
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : DB 연결 저장소 화면 — 연결 등록(지원 DB 목록 선택)·연결 테스트·목록.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 29.       | 최남희  | 표 목록 → 카드 그리드 전환. 연결 테스트를 카드 인라인으로
 * |                          | 옮기고(다이얼로그 테스트 폼은 등록/수정에만 유지), 사용 중인
 * |                          | 테이블 수를 /api/registrations와 join해 표시. "+ 연결 추가"
 * |                          | 점선 카드로 등록 다이얼로그 진입.
 * --------------------------------------------------
 */
import { useCallback, useEffect, useState } from 'react'
import { Loader2, MoreVertical, Plus } from 'lucide-react'
import { api } from '@/lib/api'
import { emptyConnection } from './types'
import type { DbConnection, DbTypeOption, TestResult } from './types'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { GhostButton } from '@/components/ui/ghost-button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { StatusPill } from '@/components/ui/status-pill'

/** /api/registrations 응답 중 카드의 "사용 중인 테이블 수" 계산에 필요한 필드만. */
interface RegisteredTableRef {
  id: number
  sourceConnectionId: number
  targetConnectionId: number
}

/** 세션 내 연결 테스트 결과 — backend가 마지막 테스트 결과를 저장하지 않으므로
 * (DbConnection 응답에 필드 없음) 새로고침하면 다시 "미테스트"로 돌아간다. */
interface RowTest {
  result: TestResult
  testedAtMs: number
}

const CARD_GRID_COLS = 'repeat(auto-fill, minmax(320px, 360px))'

/** DB 연결 저장소 — 등록·수정·삭제·연결 테스트. 지원 DB 목록은 backend가 내려준다. */
export function ConnectionsPanel() {
  const [connections, setConnections] = useState<DbConnection[]>([])
  const [dbTypes, setDbTypes] = useState<DbTypeOption[]>([])
  const [registrations, setRegistrations] = useState<RegisteredTableRef[] | null>(null)
  const [listError, setListError] = useState<string | null>(null)
  const [rowTests, setRowTests] = useState<Record<number, RowTest>>({})
  const [testingId, setTestingId] = useState<number | null>(null)

  const [open, setOpen] = useState(false)
  const [form, setForm] = useState<DbConnection>(emptyConnection)
  const [formError, setFormError] = useState<string | null>(null)
  const [formTest, setFormTest] = useState<TestResult | null>(null)
  const [busy, setBusy] = useState(false)

  const reload = useCallback(() => {
    setListError(null)
    api<DbConnection[]>('/api/connections')
      .then(setConnections)
      .catch((e: Error) => setListError(e.message))
    api<RegisteredTableRef[]>('/api/registrations')
      .then(setRegistrations)
      .catch(() => setRegistrations(null))
  }, [])

  useEffect(() => {
    reload()
    api<DbTypeOption[]>('/api/connections/db-types')
      .then(setDbTypes)
      .catch(() => setDbTypes([{ code: 'ORACLE', label: 'Oracle' }]))
  }, [reload])

  const openNew = () => {
    setForm(emptyConnection)
    setFormError(null)
    setFormTest(null)
    setOpen(true)
  }

  const openEdit = (c: DbConnection) => {
    setForm({ ...c, password: '' })
    setFormError(null)
    setFormTest(null)
    setOpen(true)
  }

  const set = (patch: Partial<DbConnection>) => setForm((f) => ({ ...f, ...patch }))

  const save = async () => {
    setBusy(true)
    setFormError(null)
    try {
      if (form.id == null) {
        await api('/api/connections', { method: 'POST', body: JSON.stringify(form) })
      } else {
        await api(`/api/connections/${form.id}`, {
          method: 'PUT',
          body: JSON.stringify(form),
        })
      }
      setOpen(false)
      reload()
    } catch (e) {
      setFormError((e as Error).message)
    } finally {
      setBusy(false)
    }
  }

  const testForm = async () => {
    setBusy(true)
    setFormTest(null)
    try {
      setFormTest(await api<TestResult>('/api/connections/test', {
        method: 'POST',
        body: JSON.stringify(form),
      }))
    } catch (e) {
      setFormTest({ ok: false, message: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  const testRow = async (c: DbConnection) => {
    if (c.id == null) return
    const id = c.id
    setTestingId(id)
    try {
      const result = await api<TestResult>(`/api/connections/${id}/test`, { method: 'POST' })
      setRowTests((r) => ({ ...r, [id]: { result, testedAtMs: Date.now() } }))
    } catch (e) {
      setRowTests((r) => ({
        ...r,
        [id]: { result: { ok: false, message: (e as Error).message }, testedAtMs: Date.now() },
      }))
    } finally {
      setTestingId(null)
    }
  }

  const remove = async (c: DbConnection) => {
    if (c.id == null) return
    if (!window.confirm(`연결 "${c.name}"을(를) 삭제할까요?`)) return
    try {
      await api(`/api/connections/${c.id}`, { method: 'DELETE' })
      reload()
    } catch (e) {
      setListError((e as Error).message)
    }
  }

  const tableCountOf = (connId: number): number | null => {
    if (registrations === null) return null
    return registrations.filter(
      (r) => r.sourceConnectionId === connId || r.targetConnectionId === connId,
    ).length
  }

  const sourceCount = connections.filter((c) => c.role === 'SOURCE').length
  const targetCount = connections.filter((c) => c.role === 'TARGET').length

  return (
    <div className="space-y-3">
      <p className="text-sm text-ink-3">
        소스 {sourceCount} · 타깃 {targetCount}
      </p>

      {listError && <p className="text-sm text-crit">{listError}</p>}

      <div className="grid gap-4" style={{ gridTemplateColumns: CARD_GRID_COLS }}>
        {connections.map((c) => (
          <ConnectionCard
            key={c.id}
            connection={c}
            tableCount={c.id != null ? tableCountOf(c.id) : null}
            test={c.id != null ? rowTests[c.id] : undefined}
            testing={c.id != null && testingId === c.id}
            onTest={() => void testRow(c)}
            onEdit={() => openEdit(c)}
            onDelete={() => void remove(c)}
          />
        ))}
        <AddConnectionCard onClick={openNew} />
      </div>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{form.id == null ? 'DB 연결 등록' : 'DB 연결 수정'}</DialogTitle>
            <DialogDescription>
              {form.id == null
                ? '소스/타깃 DB 접속 정보를 입력하세요.'
                : '비밀번호를 비워두면 기존 값이 유지됩니다.'}
            </DialogDescription>
          </DialogHeader>

          <div className="grid grid-cols-2 gap-3">
            <div className="col-span-2 space-y-1">
              <Label htmlFor="conn-name">이름</Label>
              <Input
                id="conn-name"
                value={form.name}
                onChange={(e) => set({ name: e.target.value })}
                placeholder="예: oracle-src-dev"
              />
            </div>
            <div className="space-y-1">
              <Label>DB 종류</Label>
              <Select value={form.dbType} onValueChange={(v) => set({ dbType: v })}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {dbTypes.map((t) => (
                    <SelectItem key={t.code} value={t.code}>
                      {t.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1">
              <Label>역할</Label>
              <Select
                value={form.role}
                onValueChange={(v) => set({ role: v as DbConnection['role'] })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="SOURCE">SOURCE (캡처 원본)</SelectItem>
                  <SelectItem value="TARGET">TARGET (적재 대상)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1">
              <Label htmlFor="conn-host">호스트</Label>
              <Input
                id="conn-host"
                value={form.host}
                onChange={(e) => set({ host: e.target.value })}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="conn-port">포트</Label>
              <Input
                id="conn-port"
                type="number"
                value={form.port}
                onChange={(e) => set({ port: Number(e.target.value) })}
              />
            </div>
            <div className="col-span-2 space-y-1">
              <Label htmlFor="conn-db">Service name / SID</Label>
              <Input
                id="conn-db"
                value={form.databaseName}
                onChange={(e) => set({ databaseName: e.target.value })}
                placeholder="예: XEPDB1"
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="conn-user">사용자</Label>
              <Input
                id="conn-user"
                value={form.username}
                onChange={(e) => set({ username: e.target.value })}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="conn-pw">비밀번호</Label>
              <Input
                id="conn-pw"
                type="password"
                value={form.password ?? ''}
                onChange={(e) => set({ password: e.target.value })}
              />
            </div>
          </div>

          {formTest && (
            <p className={`text-sm ${formTest.ok ? 'text-ok' : 'text-crit'}`}>
              {formTest.ok ? `연결 성공: ${formTest.message}` : `연결 실패: ${formTest.message}`}
            </p>
          )}
          {formError && <p className="text-sm text-crit">{formError}</p>}

          <DialogFooter>
            <Button variant="outline" disabled={busy} onClick={() => void testForm()}>
              연결 테스트
            </Button>
            <Button disabled={busy} onClick={() => void save()}>
              저장
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

/** 연결 카드 — 헤더 좌측 4px 액센트는 마지막 테스트 결과(성공 ok/실패 crit/미테스트 stop). */
function ConnectionCard({
  connection: c,
  tableCount,
  test,
  testing,
  onTest,
  onEdit,
  onDelete,
}: {
  connection: DbConnection
  /** null = /api/registrations 조회 실패 — "—"로 표시(0건으로 단정하지 않음) */
  tableCount: number | null
  test: RowTest | undefined
  testing: boolean
  onTest: () => void
  onEdit: () => void
  onDelete: () => void
}) {
  const accent = !test ? 'var(--stop)' : test.result.ok ? 'var(--ok)' : 'var(--crit)'

  return (
    <Card>
      <CardHeader
        className="border-l-4 bg-surface-2"
        style={{ borderLeftColor: accent }}
      >
        <CardTitle className="min-w-0 truncate">{c.name}</CardTitle>
        <StatusPill variant={c.role === 'SOURCE' ? 'brand' : 'stop'} dot={false}>
          {c.role === 'SOURCE' ? '소스' : '타깃'}
        </StatusPill>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button
              type="button"
              className="ml-auto shrink-0 rounded p-1 text-ink-3 hover:bg-card hover:text-foreground"
              title="더 보기"
            >
              <MoreVertical className="size-4" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onSelect={onEdit}>수정</DropdownMenuItem>
            <DropdownMenuItem variant="destructive" onSelect={onDelete}>
              삭제
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </CardHeader>

      <CardContent>
        <div className="grid grid-cols-[112px_1fr] gap-x-3 gap-y-1.5 text-[12.5px]">
          <span className="text-ink-3">접속</span>
          <span className="truncate font-mono text-[12px]">
            {c.host}:{c.port}/{c.databaseName}
          </span>
          <span className="text-ink-3">사용자</span>
          <span className="truncate font-mono text-[12px]">{c.username}</span>
          <span className="text-ink-3">사용 중인 테이블</span>
          <span className="font-mono text-[12px]">
            {tableCount === null ? '—' : `${tableCount}개`}
          </span>
        </div>
        {/* 소스 등록 사전 점검값(supplemental logging·archivelog 등)은 /api/registrations/
            db-checks/{id}가 실시간 조회해야 얻어지는 값이라 /api/connections 응답엔 없다.
            없는 데이터를 추정 표시하지 않기 위해 이 카드에는 생략한다. */}

        {test && !test.result.ok && (
          <div className="mt-2.5 rounded-md border border-crit/40 bg-crit-soft px-3 py-2 text-[12px] text-crit">
            {test.result.message}
          </div>
        )}
      </CardContent>

      <CardFooter>
        <span className="mr-auto font-mono text-[11px] text-ink-3">
          {test
            ? new Date(test.testedAtMs).toLocaleTimeString('ko-KR', { hour12: false })
            : '미테스트'}
        </span>
        <GhostButton onClick={onTest} disabled={testing}>
          {testing && <Loader2 className="size-3.5 animate-spin" />}
          테스트
        </GhostButton>
      </CardFooter>
    </Card>
  )
}

/** "+ 연결 추가" 점선 카드 — 그리드 마지막 셀, 클릭 시 등록 다이얼로그를 연다. */
function AddConnectionCard({ onClick }: { onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex min-h-[168px] flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-line-2 text-ink-3 transition-colors hover:border-primary hover:text-primary"
    >
      <Plus className="size-5" />
      <span className="text-[12.5px] font-medium">연결 추가</span>
    </button>
  )
}

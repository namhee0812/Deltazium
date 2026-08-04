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
 */
import { useCallback, useEffect, useState } from 'react'
import { api } from '@/lib/api'
import { emptyConnection } from './types'
import type { DbConnection, DbTypeOption, TestResult } from './types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

/** DB 연결 저장소 — 등록·수정·삭제·연결 테스트. 지원 DB 목록은 backend가 내려준다. */
export function ConnectionsPanel() {
  const [connections, setConnections] = useState<DbConnection[]>([])
  const [dbTypes, setDbTypes] = useState<DbTypeOption[]>([])
  const [listError, setListError] = useState<string | null>(null)
  const [rowResults, setRowResults] = useState<Record<number, TestResult>>({})

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
    try {
      const result = await api<TestResult>(`/api/connections/${id}/test`, { method: 'POST' })
      setRowResults((r) => ({ ...r, [id]: result }))
    } catch (e) {
      setRowResults((r) => ({ ...r, [id]: { ok: false, message: (e as Error).message } }))
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

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          CDC 소스/타깃으로 쓸 DB 접속 정보를 등록합니다.
        </p>
        <Button size="sm" onClick={openNew}>
          연결 추가
        </Button>
      </div>

      {listError && <p className="text-sm text-destructive">{listError}</p>}

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>이름</TableHead>
            <TableHead>DB</TableHead>
            <TableHead>역할</TableHead>
            <TableHead>접속</TableHead>
            <TableHead>계정</TableHead>
            <TableHead>테스트</TableHead>
            <TableHead className="text-right">동작</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {connections.length === 0 && (
            <TableRow>
              <TableCell colSpan={7} className="text-center text-muted-foreground">
                등록된 연결이 없습니다
              </TableCell>
            </TableRow>
          )}
          {connections.map((c) => (
            <TableRow key={c.id}>
              <TableCell className="font-medium">{c.name}</TableCell>
              <TableCell>{c.dbType}</TableCell>
              <TableCell>
                <Badge variant={c.role === 'SOURCE' ? 'default' : 'secondary'}>
                  {c.role}
                </Badge>
              </TableCell>
              <TableCell className="font-mono text-xs">
                {c.host}:{c.port}/{c.databaseName}
              </TableCell>
              <TableCell>{c.username}</TableCell>
              <TableCell>
                {c.id != null && rowResults[c.id] ? (
                  <Badge
                    variant={rowResults[c.id].ok ? 'default' : 'destructive'}
                    title={rowResults[c.id].message}
                  >
                    {rowResults[c.id].ok ? '성공' : '실패'}
                  </Badge>
                ) : (
                  <span className="text-xs text-muted-foreground">—</span>
                )}
              </TableCell>
              <TableCell className="space-x-1 text-right">
                <Button variant="outline" size="sm" onClick={() => void testRow(c)}>
                  테스트
                </Button>
                <Button variant="outline" size="sm" onClick={() => openEdit(c)}>
                  수정
                </Button>
                <Button variant="destructive" size="sm" onClick={() => void remove(c)}>
                  삭제
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

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
            <p className={`text-sm ${formTest.ok ? 'text-green-600' : 'text-destructive'}`}>
              {formTest.ok ? `연결 성공: ${formTest.message}` : `연결 실패: ${formTest.message}`}
            </p>
          )}
          {formError && <p className="text-sm text-destructive">{formError}</p>}

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

/**
 * 파일명 : types.ts
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : DB 연결 도메인 타입 정의.
 *
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
export interface DbConnection {
  id: number | null
  name: string
  dbType: string
  role: 'SOURCE' | 'TARGET'
  host: string
  port: number
  databaseName: string
  username: string
  password?: string
}

export interface DbTypeOption {
  code: string
  label: string
}

export interface TestResult {
  ok: boolean
  message: string
}

export const emptyConnection: DbConnection = {
  id: null,
  name: '',
  dbType: 'ORACLE',
  role: 'SOURCE',
  host: '',
  port: 1521,
  databaseName: '',
  username: '',
  password: '',
}

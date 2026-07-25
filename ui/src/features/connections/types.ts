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

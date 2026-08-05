import path from 'node:path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    // backend(제어면)로 프록시 — UI는 Connect에 직접 붙지 않는다
    proxy: {
      '/api': 'http://localhost:8090',
    },
    // 이 서버에서 inotify 기반 감시가 변경을 놓쳐 낡은 모듈을 서빙하는 일이
    // 반복돼(2026-08-04 2회) 폴링으로 전환 — dev 전용이라 CPU 비용 감수
    watch: {
      usePolling: true,
      interval: 1000,
    },
  },
})

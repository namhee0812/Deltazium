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
  },
})

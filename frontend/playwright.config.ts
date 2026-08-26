import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './tests',
  timeout: 60_000,
  use: {
    baseURL: process.env.SCHEMABRIDGE_UI_E2E_BASE_URL ?? 'http://127.0.0.1:5173',
    headless: true,
  },
})

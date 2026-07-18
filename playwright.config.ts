import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser',
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: 'http://127.0.0.1:32112',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command:
      'go run ./cmd/beacon --db :memory: --admin-address 127.0.0.1:32112 --data-address 127.0.0.1:38080 --log-level error',
    url: 'http://127.0.0.1:32112/health',
    timeout: 120_000,
  },
});

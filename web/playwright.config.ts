import { defineConfig } from "@playwright/test";

// Locally, PW_CHANNEL=chrome uses the installed Google Chrome; CI downloads Chromium.
export default defineConfig({
  testDir: "tests/e2e",
  timeout: 60_000,
  retries: 0,
  reporter: [["list"]],
  use: {
    baseURL: process.env.E2E_BASE_URL ?? "http://localhost:8081",
    channel: process.env.PW_CHANNEL || undefined,
    viewport: { width: 1440, height: 900 },
    ignoreHTTPSErrors: true, // Caddy's internal CA when SITE_ADDRESS=localhost
  },
});

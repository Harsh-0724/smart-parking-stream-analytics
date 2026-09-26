import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

const API = process.env.API_URL ?? "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": API,
      "/ws": { target: API.replace("http", "ws"), ws: true },
    },
  },
  build: { sourcemap: false, chunkSizeWarningLimit: 250 },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["tests/setup.ts"],
    include: ["tests/**/*.test.{ts,tsx}"],
    css: true,
  },
});

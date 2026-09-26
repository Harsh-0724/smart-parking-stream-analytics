import { expect, test } from "@playwright/test";

// Runs against the real stack (Caddy -> API -> Kafka/TimescaleDB, simulator producing).
// These tests check that data is LIVE, not just that the page mounts.

const number = (text: string | null) => Number((text ?? "").replace(/[^\d.]/g, ""));

test("Overview renders live pipeline data", async ({ page }) => {
  const frames: string[] = [];
  page.on("websocket", (ws) => ws.on("framereceived", (f) => frames.push(String(f.payload))));

  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();

  // Real lots from TimescaleDB, ranked in a table
  const rows = page.locator("table tbody tr");
  await expect(rows.first()).toBeVisible({ timeout: 120_000 });
  expect(await rows.count()).toBeGreaterThanOrEqual(6);

  // The big number is the sum of live occupancy and capacity is non-zero
  const capacity = number(await page.locator(".stat--xl .stat__unit").textContent());
  expect(capacity).toBeGreaterThan(0);

  // Connected over the WebSocket
  await expect(page.getByRole("status").filter({ hasText: "Live" })).toBeVisible();

  // Real Kafka throughput is shown (from the pipeline stream), not a placeholder dash
  const eventsPerSecond = page.locator(".strip .stat", { hasText: "Events / s" }).locator(".stat__value");
  await expect.poll(async () => number(await eventsPerSecond.textContent()), { timeout: 30_000 }).toBeGreaterThan(0);

  // Data really moves: the server keeps pushing occupancy and pipeline frames
  await expect.poll(() => frames.filter((f) => f.includes('"type":"pipeline"')).length, { timeout: 30_000 }).toBeGreaterThanOrEqual(3);
  await expect.poll(() => frames.filter((f) => f.includes('"type":"occupancy"')).length, { timeout: 60_000 }).toBeGreaterThanOrEqual(1);
});

test("Lot detail floor plan receives a live snapshot", async ({ page }) => {
  await page.goto("/");
  await page.locator("table tbody tr a").first().click({ timeout: 120_000 });
  const plan = page.getByRole("img", { name: /^Floor plan:/ });
  await expect(plan).toBeVisible();
  // Slots are drawn as filled / outlined squares once the WebSocket snapshot arrives
  await expect.poll(async () => page.locator('.slotgrid [data-status="occupied"], .slotgrid [data-status="free"]').count(), { timeout: 30_000 }).toBeGreaterThan(10);
});

test("Pipeline screen shows consumer groups, partitions and brokers", async ({ page }) => {
  await page.goto("/pipeline");
  await expect(page.getByRole("heading", { name: "Pipeline" })).toBeVisible();
  await expect(page.getByText("occupancy-processor").first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText("timescale-sink").first()).toBeVisible();
  await expect(page.locator(".pmap__cell")).toHaveCount(6);
  await expect(page.getByText("kafka-1")).toBeVisible();
  await expect(page.getByText("Up").first()).toBeVisible();
});

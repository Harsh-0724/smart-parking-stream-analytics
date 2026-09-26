import { execSync } from "node:child_process";
import { expect, test } from "@playwright/test";

// Destructive: kills a processor and a broker on the running stack, then checks that the UI shows the
// failure and the recovery. Opt-in:  E2E_DESTRUCTIVE=1 pnpm exec playwright test recovery
test.skip(!process.env.E2E_DESTRUCTIVE, "set E2E_DESTRUCTIVE=1 to kill containers");

const sh = (cmd: string) => execSync(cmd, { cwd: "..", encoding: "utf8" }).trim();
const number = (t: string | null) => Number((t ?? "").replace(/[^\d.]/g, ""));

test.describe.configure({ mode: "serial" });

test("killing a processor shows a rebalance and the survivor takes over", async ({ page }) => {
  await page.goto("/pipeline");
  const cells = page.locator(".pmap__cell").first().locator("xpath=..");
  const groupCells = page.getByLabel("Consumer group occupancy-processor").locator(".pmap__cell");
  await expect(groupCells).toHaveCount(6, { timeout: 60_000 });
  const owners = async () => new Set(await groupCells.locator(".pmap__owner").allTextContents());
  await expect.poll(async () => (await owners()).size, { timeout: 60_000 }).toBe(2);
  void cells;

  const victim = sh("docker compose ps -q processor").split("\n")[0]!;
  sh(`docker kill ${victim}`);

  // Rebalancing is visible in the UI, then a single owner holds all 6 partitions
  await expect(page.locator(".banner")).toBeVisible({ timeout: 45_000 });
  await expect.poll(async () => (await owners()).size, { timeout: 60_000 }).toBe(1);
  await expect(page.getByText(/rebalance finished|assignment changed/).first()).toBeVisible();

  // The survivor restored state from the changelog (the log shows it), and data keeps flowing
  await expect
    .poll(() => sh("docker compose logs --no-log-prefix --since 3m processor 2>&1"), { timeout: 30_000 })
    .toMatch(/"restored_lots": \["LOT-[^\]]+\].*partition assigned/);
  await page.goto("/");
  const eps = page.locator(".strip .stat", { hasText: "Events / s" }).locator(".stat__value");
  await expect.poll(async () => number(await eps.textContent()), { timeout: 30_000 }).toBeGreaterThan(0);

  // Bring the second processor back: partitions move again (cooperative rebalance)
  sh("docker compose up -d --scale processor=2 processor");
  await page.goto("/pipeline");
  await expect.poll(async () => (await owners()).size, { timeout: 90_000 }).toBe(2);
});

test("killing a broker shows it Down, leaders move, and data keeps flowing", async ({ page }) => {
  await page.goto("/pipeline");
  const leads = (id: number) => page.getByRole("row", { name: new RegExp(`kafka-${id}\\b`) }).locator("td").nth(1);
  await expect(leads(1)).toBeVisible({ timeout: 30_000 });
  const counts = async () => Promise.all([1, 2, 3].map(async (id) => number(await leads(id).textContent())));
  await expect.poll(async () => (await counts()).reduce((a, b) => a + b, 0), { timeout: 30_000 }).toBeGreaterThan(0);

  // Kill the broker that currently leads the most partitions (the most interesting failure)
  const before = await counts();
  const total = before.reduce((a, b) => a + b, 0);
  const victim = before.indexOf(Math.max(...before)) + 1;
  const survivors = [1, 2, 3].filter((id) => id !== victim);
  sh(`docker kill kafka-${victim}`);

  await expect(page.getByRole("row", { name: new RegExp(`kafka-${victim}\\b`) }).getByText(/Down/)).toBeVisible({ timeout: 45_000 });
  // Its partitions were re-elected onto the two surviving brokers
  await expect
    .poll(async () => (await Promise.all(survivors.map(async (id) => number(await leads(id).textContent())))).reduce((a, b) => a + b, 0), { timeout: 60_000 })
    .toBe(total);
  // Pipeline keeps moving with one broker down (RF 3, min ISR 2)
  await page.goto("/");
  const eps = page.locator(".strip .stat", { hasText: "Events / s" }).locator(".stat__value");
  await expect.poll(async () => number(await eps.textContent()), { timeout: 30_000 }).toBeGreaterThan(0);
  await expect(page.locator(".strip .stat", { hasText: "Brokers up" }).locator(".stat__value")).toContainText("2");

  sh(`docker compose start kafka-${victim}`);
  await page.goto("/pipeline");
  await expect(page.getByRole("row", { name: new RegExp(`kafka-${victim}\\b`) }).getByText("Up")).toBeVisible({ timeout: 90_000 });
});

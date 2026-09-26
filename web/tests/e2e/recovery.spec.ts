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
  await expect(page.getByText("Rebalancing").first()).toBeVisible({ timeout: 45_000 });
  await expect.poll(async () => (await owners()).size, { timeout: 60_000 }).toBe(1);
  await expect(page.getByText(/rebalance finished|assignment changed/).first()).toBeVisible();

  // The survivor restored state from the changelog (the log shows it), and data keeps flowing
  const logs = sh("docker compose logs --no-log-prefix --since 2m processor");
  expect(logs).toMatch(/partition assigned.*restored_lots/);
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
  await expect(page.getByText("kafka-2")).toBeVisible({ timeout: 30_000 });
  sh("docker kill kafka-2");

  const row = page.getByRole("row", { name: /kafka-2/ });
  await expect(row.getByText(/Down/)).toBeVisible({ timeout: 45_000 });
  // Its partitions were re-elected onto the two surviving brokers
  const leads = (id: string) => page.getByRole("row", { name: new RegExp(`kafka-${id}`) }).locator("td").nth(1);
  await expect.poll(async () => number(await leads("1").textContent()) + number(await leads("3").textContent()), { timeout: 45_000 }).toBe(22);
  // Pipeline keeps moving with one broker down (RF 3, min ISR 2)
  await page.goto("/");
  await expect(page.getByText("2", { exact: true }).first()).toBeVisible();
  const eps = page.locator(".strip .stat", { hasText: "Events / s" }).locator(".stat__value");
  await expect.poll(async () => number(await eps.textContent()), { timeout: 30_000 }).toBeGreaterThan(0);

  sh("docker compose start kafka-2");
  await page.goto("/pipeline");
  await expect(page.getByRole("row", { name: /kafka-2/ }).getByText("Up")).toBeVisible({ timeout: 90_000 });
});

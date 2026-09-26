// Fails if the JavaScript needed for first paint exceeds the budget (gzipped).
// "Initial" = the scripts and modulepreloads referenced from index.html; lazy route chunks are excluded.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { gzipSync } from "node:zlib";

const BUDGET_KB = 200;
const dist = new URL("../dist/", import.meta.url).pathname;
const html = readFileSync(join(dist, "index.html"), "utf8");
const files = [...html.matchAll(/(?:src|href)="\/(assets\/[^"]+\.js)"/g)].map((m) => m[1]);
if (files.length === 0) throw new Error("no scripts found in dist/index.html");

let total = 0;
for (const f of files) {
  const kb = gzipSync(readFileSync(join(dist, f))).length / 1024;
  total += kb;
  console.log(`${kb.toFixed(1).padStart(7)} kB gz  ${f}`);
}
console.log(`${total.toFixed(1).padStart(7)} kB gz  initial JS (budget ${BUDGET_KB} kB)`);
if (total > BUDGET_KB) {
  console.error("Initial JS exceeds the budget");
  process.exit(1);
}

import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { chromium } from "playwright";

const baseURL = process.env.BEACON_SCREENSHOT_URL;
if (!baseURL) {
  throw new Error("BEACON_SCREENSHOT_URL must point to a disposable Beacon screenshot fixture");
}
const outputDir = resolve("internal/ui/assets/manual");

await mkdir(outputDir, { recursive: true });

const browser = await chromium.launch();
const context = await browser.newContext({
  baseURL,
  colorScheme: "light",
  deviceScaleFactor: 1,
  reducedMotion: "reduce",
  viewport: { width: 1440, height: 1100 },
});
const page = await context.newPage();
let sourceRestore;

async function captureRange(startSelector, endSelector, filename) {
  const start = await page.locator(startSelector).boundingBox();
  const end = await page.locator(endSelector).boundingBox();
  if (!start || !end) throw new Error(`Could not measure ${startSelector} through ${endSelector}`);
  await page.screenshot({
    path: resolve(outputDir, filename),
    clip: {
      x: Math.max(0, Math.min(start.x, end.x)),
      y: Math.max(0, start.y),
      width: Math.min(1440, Math.max(start.x + start.width, end.x + end.width) - Math.min(start.x, end.x)),
      height: end.y + end.height - start.y,
    },
  });
}

try {
  const health = await context.request.get("/health");
  if (!health.ok()) throw new Error(`${baseURL}/health returned ${health.status()}`);
  const configResponse = await context.request.get("/api/v1/config/export");
  if (!configResponse.ok()) throw new Error(`${baseURL}/api/v1/config/export returned ${configResponse.status()}`);
  const fixture = await configResponse.json();
  sourceRestore = fixture.sources?.find((source) => source.id === "voyage-replay");
  const hasNavigation = fixture.connectors?.some((connector) => connector.id === "navigation");
  if (!sourceRestore || !hasNavigation) {
    throw new Error("Screenshot fixture must contain source voyage-replay and connector navigation");
  }

  await page.goto("/dashboard");
  await page.getByRole("heading", { name: "Data flow" }).waitFor();
  await page.locator(".dag-board").waitFor();
  await captureRange(".home-flow-header", ".dag-board", "dashboard.png");

  await page.getByRole("link", { name: "New source" }).click();
  const sourceDialog = page.getByRole("dialog", { name: "Add source" });
  await sourceDialog.waitFor();
  await sourceDialog.locator("#src-name").fill("Engine room CAN");
  await sourceDialog.locator("#src-interface").fill("can0");
  await sourceDialog.screenshot({ path: resolve(outputDir, "source-dialog.png") });
  await sourceDialog.getByRole("button", { name: "Cancel" }).click();

  await page.goto("/connectors/navigation/");
  await page.getByRole("link", { name: "Edit connector" }).click();
  const connectorDialog = page.getByRole("dialog", { name: "Edit connector navigation" });
  await connectorDialog.waitFor();
  await connectorDialog.locator("[data-cel-validation-feedback]").getByText("filters OK").waitFor();
  await connectorDialog.screenshot({ path: resolve(outputDir, "connector-dialog.png") });

  await page.goto("/sources/voyage-replay/");
  const streamPanel = page.locator("[data-stream-panel]");
  await streamPanel.getByRole("button", { name: "Start" }).click();
  const replayResponse = await context.request.put("/api/v1/sources/voyage-replay", {
    data: {
      id: "voyage-replay",
      name: "Voyage capture",
      type: "file",
      enabled: true,
      file_path: resolve("data/on2k.log.gz"),
    },
  });
  if (!replayResponse.ok()) throw new Error(`Could not start capture replay: ${replayResponse.status()}`);
  await streamPanel.locator(".stream-message").first().waitFor({ timeout: 10_000 }).catch(() => {});
  await streamPanel.screenshot({ path: resolve(outputDir, "stream-inspector.png") });

  await page.goto("/api/docs");
  await page.getByText("beacon config API", { exact: true }).waitFor({ timeout: 20_000 });
  await page.screenshot({ path: resolve(outputDir, "api-reference.png") });
} finally {
  if (sourceRestore) {
    await context.request.put("/api/v1/sources/voyage-replay", { data: sourceRestore }).catch(() => {});
  }
  await browser.close();
}

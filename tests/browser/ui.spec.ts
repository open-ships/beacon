import { expect, test } from '@playwright/test';
import { readFile } from 'node:fs/promises';

test('operator can create and visually trace a status-colored pipeline', async ({ page }) => {
  const sourceID = `browser-test-${Date.now()}`;
  const sourceName = `Playwright source ${sourceID}`;
  const sinkID = `${sourceID}-sink`;
  const sinkName = `Playwright sink ${sourceID}`;
  const connectorID = `${sourceID}-connector`;
  const connectorName = `Playwright connector ${sourceID}`;

  await page.goto('/');

  await expect(page).toHaveURL(/\/dashboard$/);
  await expect(page).toHaveTitle('Home - beacon');
  await expect(page.getByRole('heading', { name: 'Data flow' })).toBeVisible();
  await expect(page.getByText('Add your first source')).toBeVisible();

  await page.getByRole('link', { name: 'Add a source', exact: true }).click();
  await expect(page).toHaveURL(/\/sources\/new$/);
  await expect(page).toHaveTitle('Sources - beacon');
  await expect(page.getByRole('heading', { name: 'Add source' })).toBeVisible();

  await page.locator('#src-id').fill(sourceID);
  await page.locator('#src-name').fill(sourceName);
  await page.locator('#src-interface').fill('can0');
  await page.getByRole('button', { name: 'Save' }).click();

  await expect(page).toHaveURL(/\/dashboard$/);
  await expect(page.getByRole('status')).toContainText(`Source "${sourceID}" created`);
  const sourceLink = page.getByRole('link', { name: sourceName });
  await expect(sourceLink).toBeVisible();
  const sourceRow = sourceLink.locator('xpath=ancestor::tr');
  await expect(sourceRow).toHaveClass(/component-status-row state-(up|degraded|error|restarting|disabled)/);
  expect(await sourceRow.evaluate((row) => getComputedStyle(row).backgroundColor)).not.toBe('rgba(0, 0, 0, 0)');

  await page.getByRole('link', { name: 'New sink', exact: true }).click();
  await page.locator('#sink-id').fill(sinkID);
  await page.locator('#sink-name').fill(sinkName);
  await page.locator('#sink-type').selectOption('tcp');
  await expect(page.locator('#sink-address')).toBeVisible();
  await page.locator('#sink-address').fill('127.0.0.1:0');
  await page.getByRole('button', { name: 'Save' }).click();
  await expect(page).toHaveURL(/\/dashboard$/);

  await page.getByRole('link', { name: 'New connector', exact: true }).click();
  await page.locator('#conn-id').fill(connectorID);
  await page.locator('#conn-name').fill(connectorName);
  await page.locator('#conn-source').selectOption(sourceID);
  await page.locator('#conn-sink').selectOption(sinkID);
  await page.getByRole('button', { name: 'Save' }).click();
  await expect(page).toHaveURL(/\/dashboard$/);

  const connectorNode = page.locator(`[data-connector-id="${connectorID}"]`);
  await expect(connectorNode).toHaveClass(/component-status-surface state-(up|degraded|error|restarting|disabled)/);
  expect(await connectorNode.evaluate((node) => getComputedStyle(node).backgroundColor)).not.toBe('rgba(0, 0, 0, 0)');

  const connectorState = ((await connectorNode.getAttribute('class')) ?? '').match(/state-(up|degraded|error|restarting|disabled)/)?.[1];
  expect(connectorState).toBeTruthy();
  const routeLinks = connectorNode.locator('xpath=ancestor::div[contains(@class,"dag-row")]').locator('.dag-link');
  await expect(routeLinks).toHaveCount(2);
  for (const routeLink of await routeLinks.all()) {
    await expect(routeLink).toHaveClass(new RegExp(`state-${connectorState}`));
    expect(await routeLink.evaluate((link) => getComputedStyle(link, '::before').backgroundColor)).not.toBe('rgba(0, 0, 0, 0)');
  }

  await page.goto('/connectors');
  const connectorRow = page.getByRole('link', { name: connectorName }).locator('xpath=ancestor::tr');
  await expect(connectorRow).toHaveClass(/component-status-row state-(up|degraded|error|restarting|disabled)/);
  expect(await connectorRow.evaluate((row) => getComputedStyle(row).backgroundColor)).not.toBe('rgba(0, 0, 0, 0)');
});

test('connector CEL editor provides autocomplete and live diagnostics', async ({ page }) => {
  await page.goto('/connectors/new');

  const filters = page.locator('#conn-filters');
  const completions = page.getByRole('listbox', { name: 'CEL completions' });

  await filters.fill('msg.p');
  await expect(completions).toBeVisible();
  await expect(page.getByRole('option', { name: /msg\.pgn/ })).toBeVisible();
  await filters.press('Enter');
  await expect(filters).toHaveValue('msg.pgn');

  await filters.fill('msg.pgn == 12725');
  await expect(page.getByRole('option', { name: /127250.*Vessel Heading/ })).toBeVisible();
  await filters.press('Enter');
  await expect(filters).toHaveValue('msg.pgn == 127250');

  await filters.fill('msg.payload.speedW');
  await expect(page.getByRole('option', { name: /msg\.payload\.speedWaterReferenced\s/ })).toBeVisible();
  await filters.press('Enter');
  await expect(filters).toHaveValue('msg.payload.speedWaterReferenced');

  await filters.fill('');
  await filters.press('Control+Space');
  await expect(completions).toBeVisible();
  await expect(page.getByRole('option', { name: /has\(\).*optional field/ })).toBeVisible();

  await filters.fill('msg.pgn == @');
  await expect(filters).toBeFocused();
  await expect(filters).toHaveAttribute('aria-invalid', 'true');
  const badCharacter = page.locator('.cel-filter-error');
  await expect(badCharacter).toBeVisible();
  await expect(badCharacter).toHaveText('@');
  await expect(page.locator('#filter-feedback')).toContainText('token recognition error');
  await expect(badCharacter).toHaveCSS('text-decoration-style', 'wavy');
  await expect(badCharacter).toHaveCSS('text-decoration-color', 'rgb(180, 35, 24)');

  await filters.fill('msg.pgn == 127250');
  await expect(filters).toHaveAttribute('aria-invalid', 'false');
  await expect(page.locator('.cel-filter-error')).toHaveCount(0);
  await expect(page.locator('#filter-feedback')).toContainText('filters OK');
});

test('source and sink overviews provide controllable JSON and CAN stream inspectors', async ({ page }) => {
  await page.context().grantPermissions(['clipboard-read', 'clipboard-write'], {
    origin: 'http://127.0.0.1:32112',
  });
  const suffix = Date.now();
  const sourceID = `stream-source-${suffix}`;
  const sinkID = `stream-sink-${suffix}`;
  const envelope = {
    payload: {
      info: {
        timestamp: '2026-06-29T10:10:17.530566931-04:00',
        receivedAt: '2026-07-25T08:41:35.729702-04:00',
        adapterId: 'can0',
        networkId: 'can0',
        direction: 'received',
        priority: 5,
        pgn: 130314,
        sourceId: 6,
        targetId: null,
      },
      instance: 0,
      source: 0,
      pressure: 1020690,
    },
    metadata: {
      observed_at: '2026-07-25T12:41:35.729721Z',
      pgn_name: 'Actual Pressure',
      decode: { status: 'decoded', complete: true },
    },
    raw: '/wAAEg==',
  };
  const envelopeDocument = JSON.stringify(envelope).replace(
    '"pressure":1020690',
    '"pressure":1020690,"serialNumber":18446744073709551615',
  );
  let streamBody = `data: ${envelopeDocument}\n\n`;
  let streamRequests = 0;
  let requestedFilter: string | null = null;
  const requestedFilters: Array<string | null> = [];

  await page.goto('/sources/new');
  await page.locator('#src-id').fill(sourceID);
  await page.locator('#src-name').fill('Stream source');
  await page.locator('#src-interface').fill('can0');
  await page.getByRole('button', { name: 'Save' }).click();
  await expect(page).toHaveURL(/\/dashboard$/);

  await page.route('**/ui/streams/sources/**', async (route) => {
    streamRequests += 1;
    requestedFilter = new URL(route.request().url()).searchParams.get('filter');
    requestedFilters.push(requestedFilter);
    await route.fulfill({
      status: 200,
      contentType: 'text/event-stream',
      headers: { 'Cache-Control': 'no-store' },
      body: streamBody,
    });
  });
  await page.goto(`/sources/${sourceID}/`);

  const sourcePanel = page.locator('[data-stream-panel]');
  const streamFilter = sourcePanel.getByRole('textbox', { name: 'CEL stream filter' });
  const startButton = sourcePanel.getByRole('button', { name: 'Start', exact: true });
  const stopButton = sourcePanel.getByRole('button', { name: 'Stop', exact: true });
  const jsonViewButton = sourcePanel.getByRole('button', { name: 'JSONL', exact: true });
  const canViewButton = sourcePanel.getByRole('button', { name: 'CAN bytes', exact: true });
  await expect(sourcePanel.getByRole('heading', { name: 'Stream contents' })).toBeVisible();
  await expect(sourcePanel.getByText('Stopped · 0 captured')).toBeVisible();
  await expect(jsonViewButton).toHaveAttribute('aria-pressed', 'true');
  await expect(jsonViewButton).toHaveClass(/btn-primary/);
  await expect(canViewButton).toHaveAttribute('aria-pressed', 'false');
  await expect(canViewButton).toHaveClass(/btn-ghost/);
  await expect(startButton).toBeVisible();
  await expect(stopButton).toBeHidden();
  expect(await streamFilter.evaluate((input) => (
    input.closest('.stream-filter-control')?.nextElementSibling?.hasAttribute('data-stream-start')
  ))).toBe(true);
  const filterBounds = await streamFilter.boundingBox();
  const startBounds = await startButton.boundingBox();
  expect(filterBounds).not.toBeNull();
  expect(startBounds).not.toBeNull();
  expect(filterBounds!.x + filterBounds!.width).toBeLessThanOrEqual(startBounds!.x);
  await streamFilter.fill('msg.pgn == 130314');
  await startButton.click();
  await expect.poll(() => requestedFilter).toBe('msg.pgn == 130314');
  await expect(startButton).toBeHidden();
  await expect(stopButton).toBeVisible();
  await expect(streamFilter).toBeEnabled();
  await expect(sourcePanel.locator('.stream-message-content')).toContainText('"pressure":1020690');
  await expect(sourcePanel.locator('.stream-message-content')).toContainText(
    '"serialNumber":18446744073709551615',
  );
  await streamFilter.fill('msg.source == 6');
  await expect.poll(() => requestedFilters.at(-1)).toBe('msg.source == 6');
  await expect(sourcePanel.locator('[data-stream-status]')).toContainText('2 captured');
  const requestsBeforeInvalidLiveFilter = streamRequests;
  await streamFilter.fill('msg.pgn == @');
  await expect(sourcePanel.locator('[data-stream-filter-feedback]')).toContainText(
    'current stream filter is unchanged',
  );
  expect(streamRequests).toBe(requestsBeforeInvalidLiveFilter);
  await expect(stopButton).toBeVisible();
  await streamFilter.fill('msg.source == 6');
  await expect(streamFilter).toHaveAttribute('aria-invalid', 'false');
  await stopButton.click();
  await expect(sourcePanel.locator('[data-stream-status]')).toContainText('Stopped');
  await expect(startButton).toBeVisible();
  await expect(stopButton).toBeHidden();
  const requestsBeforeInvalidFilter = streamRequests;
  await streamFilter.fill('msg.pgn == @');
  await startButton.click();
  await expect(streamFilter).toHaveAttribute('aria-invalid', 'true');
  await expect(sourcePanel.locator('[data-stream-filter-feedback]')).toContainText('Invalid CEL');
  expect(streamRequests).toBe(requestsBeforeInvalidFilter);
  await streamFilter.fill('msg.pgn == 130314');

  const celTarget = sourcePanel.locator(
    '[data-cel-path="msg.payload.pressure"][data-cel-expression="msg.payload.pressure == 1020690"]',
  ).first();
  await expect(celTarget).toBeVisible();
  await sourcePanel.locator('.stream-message-content').first().evaluate((content) => content.click());
  await expect(sourcePanel.locator('[data-stream-cel-expression]')).toHaveText('has(msg.payload)');
  await celTarget.click();
  const celInspector = sourcePanel.locator('[data-stream-cel-inspector]');
  await expect(celInspector).toBeVisible();
  await expect(celInspector.locator('[data-stream-cel-path]')).toHaveText('msg.payload.pressure');
  await expect(celInspector.locator('[data-stream-cel-expression]')).toHaveText(
    'msg.payload.pressure == 1020690',
  );
  await celInspector.getByRole('button', { name: 'Use filter', exact: true }).click();
  await expect(streamFilter).toHaveValue('msg.payload.pressure == 1020690');
  await streamFilter.fill('msg.pgn == 130314');

  const visibleJSONLines = await sourcePanel.locator('.stream-message-content').allTextContents();
  await sourcePanel.getByRole('button', { name: 'Copy stream', exact: true }).click();
  const copiedJSONL = await page.evaluate(() => navigator.clipboard.readText());
  expect(copiedJSONL.trimEnd().split('\n')).toEqual(visibleJSONLines.slice().reverse());
  expect(copiedJSONL).toContain('"serialNumber":18446744073709551615');

  await canViewButton.click();
  await expect(canViewButton).toHaveAttribute('aria-pressed', 'true');
  await expect(canViewButton).toHaveClass(/btn-primary/);
  await expect(jsonViewButton).toHaveAttribute('aria-pressed', 'false');
  await expect(jsonViewButton).toHaveClass(/btn-ghost/);
  await expect(sourcePanel.locator('.stream-message-content').first()).toContainText('FF 00 00 12');
  expect(await sourcePanel.locator('.stream-message-content').first().textContent()).not.toContain('\n');
  await sourcePanel.getByRole('button', { name: 'Copy stream', exact: true }).click();
  const copiedCAN = await page.evaluate(() => navigator.clipboard.readText());
  expect(copiedCAN.trimEnd().split('\n')).toEqual(
    Array.from({ length: visibleJSONLines.length }, () => 'FF 00 00 12'),
  );

  const jsonDownloadPromise = page.waitForEvent('download');
  await sourcePanel.getByRole('button', { name: 'Export JSONL', exact: true }).click();
  const jsonDownload = await jsonDownloadPromise;
  expect(jsonDownload.suggestedFilename()).toContain(`${sourceID}-`);
  expect(jsonDownload.suggestedFilename()).toMatch(/\.n2k\.jsonl$/);
  const jsonPath = await jsonDownload.path();
  expect(jsonPath).not.toBeNull();
  expect(await readFile(jsonPath!, 'utf8')).toContain('"pressure":1020690');
  expect(await readFile(jsonPath!, 'utf8')).toContain('"serialNumber":18446744073709551615');

  const canDownloadPromise = page.waitForEvent('download');
  await sourcePanel.getByRole('button', { name: 'Export CAN', exact: true }).click();
  const canDownload = await canDownloadPromise;
  expect(canDownload.suggestedFilename()).toMatch(/\.can\.hex$/);
  const canPath = await canDownload.path();
  expect(canPath).not.toBeNull();
  expect(await readFile(canPath!, 'utf8')).toBe(
    Array.from({ length: visibleJSONLines.length }, () => 'FF000012').join('\n') + '\n',
  );

  await sourcePanel.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(sourcePanel.locator('[data-stream-status]')).toContainText('0 captured');
  streamBody = Array.from({ length: 250 }, (_, index) => (
    `data: ${envelopeDocument.replace('"pressure":1020690', `"pressure":${1020690 + index}`)}\n\n`
  )).join('');
  await jsonViewButton.click();
  await expect(jsonViewButton).toHaveAttribute('aria-pressed', 'true');
  await expect(jsonViewButton).toHaveClass(/btn-primary/);
  await startButton.click();
  await expect(sourcePanel.locator('[data-stream-status]')).toContainText('250 captured');
  await expect(sourcePanel.locator('[data-stream-status]')).toContainText('latest 200 shown');
  await stopButton.click();

  const capturedMessages = sourcePanel.locator('.stream-message');
  await expect(capturedMessages).toHaveCount(200);
  await expect(capturedMessages.first().locator('.stream-message-content')).toBeVisible();
  await expect(sourcePanel.locator('[data-stream-list]')).toHaveCSS('background-color', 'rgb(255, 255, 255)');
  await expect(capturedMessages.first().locator('.stream-message-content')).toHaveCSS('color', 'rgb(17, 24, 39)');
  await expect(capturedMessages.nth(1)).toHaveCSS('border-top-style', 'none');
  const displayedLines = await capturedMessages.locator('.stream-message-content').allTextContents();
  expect(displayedLines).toHaveLength(200);
  expect(displayedLines.every((line) => !line.includes('\n') && line.startsWith('{') && line.endsWith('}'))).toBe(true);
  await sourcePanel.getByRole('button', { name: 'Copy stream', exact: true }).click();
  const copiedRetainedJSONL = await page.evaluate(() => navigator.clipboard.readText());
  expect(copiedRetainedJSONL.trimEnd().split('\n')).toHaveLength(200);
  expect(await capturedMessages.first().evaluate((message) => message.getBoundingClientRect().height)).toBeLessThan(34);
  expect(await sourcePanel.locator('[data-stream-list]').evaluate((list) => list.scrollHeight)).toBeGreaterThan(
    await sourcePanel.locator('[data-stream-list]').evaluate((list) => list.clientHeight),
  );

  await page.goto('/sinks/new');
  await page.locator('#sink-id').fill(sinkID);
  await page.locator('#sink-name').fill('Stream sink');
  await page.locator('#sink-type').selectOption('tcp');
  await page.locator('#sink-address').fill('127.0.0.1:0');
  await page.getByRole('button', { name: 'Save' }).click();
  await expect(page).toHaveURL(/\/dashboard$/);
  await page.goto(`/sinks/${sinkID}/`);

  const sinkPanel = page.locator('[data-stream-panel]');
  await expect(sinkPanel).toHaveAttribute('data-stream-url', `/ui/streams/sinks/${sinkID}`);
  await expect(sinkPanel.getByRole('button', { name: 'Start', exact: true })).toBeVisible();
  await expect(sinkPanel.getByRole('button', { name: 'Stop', exact: true })).toBeHidden();
  await expect(sinkPanel.getByRole('button', { name: 'Export JSONL', exact: true })).toBeDisabled();
  await expect(sinkPanel.getByRole('button', { name: 'Export CAN', exact: true })).toBeDisabled();
  await expect(sinkPanel.getByRole('button', { name: 'Copy stream', exact: true })).toBeDisabled();
});

test('MCP reference is available from the header and stays local', async ({ page }) => {
  const requestedOrigins = new Set<string>();
  page.on('request', (request) => requestedOrigins.add(new URL(request.url()).origin));

  await page.goto('/dashboard');
  const mcpLink = page.getByRole('navigation', { name: 'Primary' }).getByRole('link', { name: 'mcp', exact: true });
  await expect(mcpLink).toBeVisible();
  await mcpLink.click();

  await expect(page).toHaveURL(/\/mcp\/info$/);
  await expect(page).toHaveTitle('MCP - beacon');
  await expect(page.getByRole('heading', { name: 'Model Context Protocol' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Install Beacon MCP' })).toBeVisible();
  await expect(page.getByText('codex mcp add beacon --url http://127.0.0.1:2112/mcp', { exact: true })).toBeVisible();
  await expect(page.getByText('claude mcp add --transport http beacon http://127.0.0.1:2112/mcp', { exact: true })).toBeVisible();
  await expect(page.getByText('gemini mcp add beacon http://127.0.0.1:2112/mcp --transport http', { exact: true })).toBeVisible();
  await expect(page.getByText('.vscode/mcp.json', { exact: true })).toBeVisible();
  await expect(page.getByText('/mcp', { exact: true }).first()).toBeVisible();
  await expect(page.getByRole('table').getByText('get_delivery_metrics', { exact: true })).toBeVisible();
  await expect(page.getByRole('table').getByText('get_source_metrics', { exact: true })).toBeVisible();
  await expect(page.getByRole('table').getByText('commit_source_traffic_baseline', { exact: true })).toHaveCount(0);
  await expect(page.getByText('offline ready', { exact: true })).toBeVisible();
  expect([...requestedOrigins]).toEqual(['http://127.0.0.1:32112']);
});

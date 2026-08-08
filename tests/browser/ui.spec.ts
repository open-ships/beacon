import { expect, test } from '@playwright/test';
import { readFile } from 'node:fs/promises';

test('operator can create and visually trace a pipeline', async ({ page }) => {
  const runID = Date.now();
  const sourceName = `Playwright source ${runID}`;
  const sinkName = `Playwright sink ${runID}`;
  const connectorName = `Playwright connector ${runID}`;

  await page.goto('/');

  await expect(page).toHaveURL(/\/dashboard$/);
  await expect(page).toHaveTitle('Home - beacon');
  await expect(page.getByRole('heading', { name: 'Data flow' })).toBeVisible();
  await expect(page.getByText('Add your first source')).toBeVisible();

  await page.getByRole('link', { name: 'Add a source', exact: true }).click();
  await expect(page).toHaveURL(/\/dashboard$/);
  const sourceDialog = page.getByRole('dialog', { name: 'Add source' });
  await expect(sourceDialog).toBeVisible();
  const sourceID = await sourceDialog.locator('input[name="id"]').inputValue();
  expect(sourceID).toMatch(/^[0-9a-f]{8}$/);
  await expect(sourceDialog.locator('#src-id')).toHaveCount(0);
  const sourceEnabled = sourceDialog.getByRole('checkbox', { name: 'Enabled' });
  await expect(sourceEnabled).toBeChecked();
  await expect(sourceEnabled).toBeFocused();
  await expect(sourceDialog.locator('input:not([type="hidden"]), select, textarea').first()).toHaveAttribute('name', 'enabled');

  await sourceDialog.locator('#src-name').fill(sourceName);
  await sourceDialog.locator('#src-interface').fill('can0');
  await sourceDialog.getByRole('button', { name: 'Save' }).click();

  await expect(page).toHaveURL(/\/dashboard$/);
  await expect(page.getByRole('status')).toContainText(`Source "${sourceID}" created`);
  const unconnectedSourceNode = page.locator(`[data-dag-node="source:${sourceID}"]`);
  await expect(unconnectedSourceNode).toBeVisible();
  await expect(page.locator(`[data-dag-from="source:${sourceID}"]`)).toHaveCount(0);
  const sourceLink = page.getByRole('region', { name: 'Sources metadata' }).getByRole('link', { name: sourceName });
  await expect(sourceLink).toBeVisible();
  const sourceRow = sourceLink.locator('xpath=ancestor::tr');
  await expect(sourceRow).toHaveClass(/component-status-row state-(up|degraded|error|restarting|disabled)/);
  await expect(sourceRow).toHaveCSS('background-color', 'rgb(255, 255, 255)');

  await page.getByRole('link', { name: 'New sink', exact: true }).click();
  await expect(page).toHaveURL(/\/dashboard$/);
  const sinkDialog = page.getByRole('dialog', { name: 'Add sink' });
  await expect(sinkDialog).toBeVisible();
  const sinkID = await sinkDialog.locator('input[name="id"]').inputValue();
  expect(sinkID).toMatch(/^[0-9a-f]{8}$/);
  await expect(sinkDialog.locator('#sink-id')).toHaveCount(0);
  await expect(sinkDialog.getByRole('checkbox', { name: 'Enabled' })).toBeChecked();
  await sinkDialog.locator('#sink-name').fill(sinkName);
  await sinkDialog.locator('#sink-type').selectOption('tcp');
  await expect(sinkDialog.locator('#sink-address')).toBeVisible();
  await sinkDialog.locator('#sink-address').fill('127.0.0.1:0');
  await sinkDialog.getByRole('button', { name: 'Save' }).click();
  await expect(page).toHaveURL(/\/dashboard$/);
  const unconnectedSinkNode = page.locator(`[data-dag-node="sink:${sinkID}"]`);
  await expect(unconnectedSinkNode).toBeVisible();
  await expect(page.locator(`[data-dag-to="sink:${sinkID}"]`)).toHaveCount(0);

  await page.getByRole('link', { name: 'New connector', exact: true }).click();
  await expect(page).toHaveURL(/\/dashboard$/);
  const connectorDialog = page.getByRole('dialog', { name: 'Add connector' });
  await expect(connectorDialog).toBeVisible();
  const connectorID = await connectorDialog.locator('input[name="id"]').inputValue();
  expect(connectorID).toMatch(/^[0-9a-f]{8}$/);
  await expect(connectorDialog.locator('#conn-id')).toHaveCount(0);
  await expect(connectorDialog.getByRole('checkbox', { name: 'Enabled' })).toBeChecked();
  await connectorDialog.locator('#conn-name').fill(connectorName);
  await connectorDialog.locator('#conn-source').selectOption(sourceID);
  await connectorDialog.locator('#conn-sink').selectOption(sinkID);
  await connectorDialog.getByRole('button', { name: 'Save' }).click();
  await expect(page).toHaveURL(/\/dashboard$/);

  const connectorNode = page.locator(`[data-connector-id="${connectorID}"]`);
  await expect(connectorNode).toHaveClass(/component-status-surface state-(up|degraded|error|restarting|disabled)/);
  await expect(connectorNode).toHaveCSS('background-color', 'rgb(255, 255, 255)');

  const connectorState = ((await connectorNode.getAttribute('class')) ?? '').match(/state-(up|degraded|error|restarting|disabled)/)?.[1];
  expect(connectorState).toBeTruthy();
  const routeLinks = page.locator(
    `[data-dag-from="source:${sourceID}"][data-dag-to="connector:${connectorID}"], ` +
    `[data-dag-from="connector:${connectorID}"][data-dag-to="sink:${sinkID}"]`,
  );
  await expect(routeLinks).toHaveCount(2);
  for (const routeLink of await routeLinks.all()) {
    await expect(routeLink).toHaveClass(new RegExp(`state-${connectorState}`));
    await expect(routeLink).toHaveAttribute('d', /^M /);
    await expect(routeLink).not.toHaveAttribute('marker-end');
    await expect(routeLink).toHaveCSS('animation-name', 'dag-edge-flash');
    await expect(routeLink).toHaveCSS('animation-duration', '1s');
    expect(await routeLink.evaluate((link) => getComputedStyle(link).strokeDasharray)).toMatch(/^1(px)?[, ]+7(px)?$/);
    expect(await routeLink.evaluate((link) => getComputedStyle(link).stroke)).not.toBe('none');
  }

  const edgeSelectors = [
    `[data-dag-from="source:${sourceID}"][data-dag-to="connector:${connectorID}"]`,
    `[data-dag-from="connector:${connectorID}"][data-dag-to="sink:${sinkID}"]`,
  ];
  const edgeStability = await page.evaluate(async (selectors) => {
    const initialEdges = selectors.map((selector) => document.querySelector(selector));
    const widths: number[] = [];
    const opacities: number[] = [];
    let missingFrames = 0;
    let replacementSeen = false;
    // Dashboard polling intentionally runs every five seconds to keep idle CPU
    // and allocation pressure low. Observe through one complete refresh so the
    // edge replacement and animation-stability assertions remain meaningful.
    const stopAt = performance.now() + 6200;

    while (performance.now() < stopAt) {
      const edges = selectors.map((selector) => document.querySelector(selector));
      if (edges.some((edge) => !edge?.getAttribute('d'))) missingFrames += 1;
      replacementSeen ||= edges.some((edge, index) => edge !== initialEdges[index]);
      for (const edge of edges) {
        if (!edge) continue;
        const style = getComputedStyle(edge);
        widths.push(Number.parseFloat(style.strokeWidth));
        opacities.push(Number.parseFloat(style.opacity));
      }
      await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
    }

    return {
      missingFrames,
      replacementSeen,
      minWidth: Math.min(...widths),
      maxWidth: Math.max(...widths),
      minOpacity: Math.min(...opacities),
      maxOpacity: Math.max(...opacities),
    };
  }, edgeSelectors);
  expect(edgeStability.replacementSeen).toBe(true);
  expect(edgeStability.missingFrames).toBe(0);
  expect(edgeStability.minWidth).toBeGreaterThanOrEqual(2.2);
  expect(edgeStability.maxWidth).toBeLessThanOrEqual(2.3);
  expect(edgeStability.maxWidth - edgeStability.minWidth).toBeLessThanOrEqual(0.02);
  expect(edgeStability.minOpacity).toBeLessThanOrEqual(0.6);
  expect(edgeStability.maxOpacity - edgeStability.minOpacity).toBeGreaterThanOrEqual(0.25);

  await page.emulateMedia({ reducedMotion: 'reduce' });
  for (const routeLink of await routeLinks.all()) {
    await expect(routeLink).toHaveCSS('animation-name', 'none');
    await expect(routeLink).toHaveCSS('opacity', '0.8');
    expect(await routeLink.evaluate((link) => getComputedStyle(link).strokeDasharray)).toMatch(/^1(px)?[, ]+7(px)?$/);
  }
  await page.emulateMedia({ reducedMotion: 'no-preference' });

  await page.getByRole('link', { name: 'New connector', exact: true }).click();
  const secondConnectorDialog = page.getByRole('dialog', { name: 'Add connector' });
  await expect(secondConnectorDialog).toBeVisible();
  const secondConnectorID = await secondConnectorDialog.locator('input[name="id"]').inputValue();
  expect(secondConnectorID).toMatch(/^[0-9a-f]{8}$/);
  expect(secondConnectorID).not.toBe(connectorID);
  await secondConnectorDialog.locator('#conn-name').fill(`Second ${connectorName}`);
  await secondConnectorDialog.locator('#conn-source').selectOption(sourceID);
  await secondConnectorDialog.locator('#conn-sink').selectOption(sinkID);
  await secondConnectorDialog.getByRole('button', { name: 'Save' }).click();
  await expect(page).toHaveURL(/\/dashboard$/);

  await expect(page.locator(`[data-dag-node="source:${sourceID}"]`)).toHaveCount(1);
  await expect(page.locator(`[data-dag-node="sink:${sinkID}"]`)).toHaveCount(1);
  const sharedSourceEdges = page.locator(`[data-dag-from="source:${sourceID}"]`);
  const sharedSinkEdges = page.locator(`[data-dag-to="sink:${sinkID}"]`);
  await expect(sharedSourceEdges).toHaveCount(2);
  await expect(sharedSinkEdges).toHaveCount(2);
  await expect(sharedSourceEdges.first()).toHaveAttribute('d', /^M /);
  await expect(sharedSinkEdges.first()).toHaveAttribute('d', /^M /);
  const sourcePaths = await sharedSourceEdges.evaluateAll((edges) => edges.map((edge) => edge.getAttribute('d')));
  const sinkPaths = await sharedSinkEdges.evaluateAll((edges) => edges.map((edge) => edge.getAttribute('d')));
  expect(new Set(sourcePaths).size).toBe(2);
  expect(new Set(sinkPaths).size).toBe(2);

  await page.goto('/connectors');
  const connectorRow = page.getByRole('link', { name: connectorName, exact: true }).locator('xpath=ancestor::tr');
  await expect(connectorRow).toHaveClass(/component-status-row state-(up|degraded|error|restarting|disabled)/);
  await expect(connectorRow).toHaveCSS('background-color', 'rgb(255, 255, 255)');

  const connectorEditLink = connectorRow.getByRole('link', { name: 'Edit', exact: true });
  const connectorListURL = page.url();
  await connectorEditLink.click();
  await expect(page).toHaveURL(connectorListURL);
  const connectorEditDialog = page.getByRole('dialog', { name: `Edit connector ${connectorID}` });
  await expect(connectorEditDialog).toBeVisible();
  await expect(connectorEditDialog.locator('#conn-name')).toHaveValue(connectorName);
  await expect(connectorEditDialog.locator('#conn-source')).toHaveValue(sourceID);
  await expect(connectorEditDialog.locator('#conn-sink')).toHaveValue(sinkID);
  await connectorEditDialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(connectorEditDialog).toBeHidden();
  await expect(page).toHaveURL(connectorListURL);
  await expect(connectorEditLink).toBeFocused();

  await page.goto(`/sources/${sourceID}/`);
  const sourceOverviewURL = page.url();
  const sourceEditLink = page.getByRole('link', { name: 'Edit source', exact: true });
  await sourceEditLink.click();
  await expect(page).toHaveURL(sourceOverviewURL);
  const sourceEditDialog = page.getByRole('dialog', { name: `Edit source ${sourceID}` });
  await expect(sourceEditDialog).toBeVisible();
  await expect(sourceEditDialog.locator('#src-name')).toHaveValue(sourceName);
  const editedSourceName = `${sourceName} edited`;
  await sourceEditDialog.locator('#src-name').fill(editedSourceName);
  await sourceEditDialog.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page).toHaveURL(sourceOverviewURL);
  await expect(page.getByRole('heading', { name: editedSourceName, exact: true })).toBeVisible();
  await expect(sourceEditDialog).toHaveCount(0);

  await page.goto(`/sinks/${sinkID}/`);
  const sinkOverviewURL = page.url();
  const sinkEditLink = page.getByRole('link', { name: 'Edit sink', exact: true });
  await sinkEditLink.click();
  await expect(page).toHaveURL(sinkOverviewURL);
  const sinkEditDialog = page.getByRole('dialog', { name: `Edit sink ${sinkID}` });
  await expect(sinkEditDialog).toBeVisible();
  await expect(sinkEditDialog.locator('#sink-name')).toHaveValue(sinkName);
  await page.keyboard.press('Escape');
  await expect(sinkEditDialog).toBeHidden();
  await expect(page).toHaveURL(sinkOverviewURL);
  await expect(sinkEditLink).toBeFocused();
});

test('connector CEL editor provides autocomplete and live diagnostics', async ({ page }) => {
  await page.goto('/connectors/new');
  await expect(page.locator('#conn-id')).toHaveCount(0);
  expect(await page.locator('input[name="id"]').inputValue()).toMatch(/^[0-9a-f]{8}$/);
  await expect(page.getByRole('checkbox', { name: 'Enabled' })).toBeChecked();

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
  const sourceID = await page.locator('input[name="id"]').inputValue();
  expect(sourceID).toMatch(/^[0-9a-f]{8}$/);
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

  const configurationPanel = page.locator('[data-overview-configuration]');
  const statusPanel = page.locator('[aria-label="Status and metrics"]');
  const sourcePanel = page.locator('[data-stream-panel]');
  const sourceDevicesPanel = page.locator('[aria-label="Source devices"]');
  const streamFilter = sourcePanel.getByRole('textbox', { name: 'CEL stream filter' });
  const startButton = sourcePanel.getByRole('button', { name: 'Start', exact: true });
  const stopButton = sourcePanel.getByRole('button', { name: 'Stop', exact: true });
  const jsonViewButton = sourcePanel.getByRole('button', { name: 'JSONL', exact: true });
  const canViewButton = sourcePanel.getByRole('button', { name: 'CAN bytes', exact: true });
  await expect(sourcePanel.getByRole('heading', { name: 'Stream contents' })).toBeVisible();
  await expect(configurationPanel).toBeVisible();
  await expect(statusPanel).toBeVisible();
  await expect(sourceDevicesPanel).toBeVisible();
  await expect(configurationPanel.getByRole('button', { name: 'Delete', exact: true })).toBeVisible();
  const configurationBounds = await configurationPanel.boundingBox();
  const statusBounds = await statusPanel.boundingBox();
  const streamBounds = await sourcePanel.boundingBox();
  const sourceDevicesBounds = await sourceDevicesPanel.boundingBox();
  expect(configurationBounds).not.toBeNull();
  expect(statusBounds).not.toBeNull();
  expect(streamBounds).not.toBeNull();
  expect(sourceDevicesBounds).not.toBeNull();
  expect(Math.abs(configurationBounds!.y - statusBounds!.y)).toBeLessThanOrEqual(1);
  expect(configurationBounds!.x + configurationBounds!.width).toBeLessThanOrEqual(statusBounds!.x);
  expect(statusBounds!.y + statusBounds!.height).toBeLessThanOrEqual(sourceDevicesBounds!.y);
  expect(sourceDevicesBounds!.y + sourceDevicesBounds!.height).toBeLessThanOrEqual(streamBounds!.y);
  const [configurationPadding, statusPadding] = await Promise.all([
    configurationPanel.evaluate((panel) => getComputedStyle(panel).padding),
    statusPanel.evaluate((panel) => getComputedStyle(panel).padding),
  ]);
  expect(configurationPadding).toBe(statusPadding);

  const sourceDeviceTable = sourceDevicesPanel.getByRole('region', { name: 'Source devices table' });
  await expect(sourceDeviceTable).toBeVisible();
  await sourceDevicesPanel.locator('[data-source-device-row-refresh]').evaluate((trigger) => trigger.remove());
  await sourceDevicesPanel.locator('#source-device-rows').evaluate((body) => {
    body.innerHTML = `
      <tr class="source-device-row">
        <td class="source-device-address"><strong>7</strong></td>
        <td class="source-device-identity"><code>0xC0508C00E83546CE</code><br><small>identity 1394382</small></td>
        <td class="source-device-manufacturer"><strong>Simrad</strong><br><small>code 1857</small></td>
        <td class="source-device-role">Steering and Control surfaces / Main Controller</td>
        <td class="source-device-stat"><strong>0.02</strong></td>
        <td class="source-device-stat"><strong>0 B/s</strong></td>
        <td class="source-device-stat"><strong>0.0%</strong><br><small>~0.001% bus load</small></td>
        <td class="source-device-last-seen"><time>15s ago</time></td>
      </tr>`;
  });
  const representativeRole = sourceDevicesPanel.locator('.source-device-role');
  await expect(representativeRole).toHaveCSS('white-space', 'normal');
  const roleDimensions = await representativeRole.evaluate((cell) => ({
    clientWidth: cell.clientWidth,
    scrollWidth: cell.scrollWidth,
  }));
  expect(roleDimensions.scrollWidth).toBeLessThanOrEqual(roleDimensions.clientWidth);
  await expect(sourceDevicesPanel.locator('.source-device-stat').first()).toHaveCSS('text-align', 'right');

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
  await expect(capturedMessages.first().locator('.stream-message-content')).toHaveCSS('color', 'rgb(15, 18, 25)');
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
  const sinkID = await page.locator('input[name="id"]').inputValue();
  expect(sinkID).toMatch(/^[0-9a-f]{8}$/);
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

import { expect, test } from '@playwright/test';

test('operator can load the dashboard and create a source', async ({ page }) => {
  const sourceID = `browser-test-${Date.now()}`;
  const sourceName = `Playwright source ${sourceID}`;

  await page.goto('/');

  await expect(page).toHaveURL(/\/ui\/dashboard$/);
  await expect(page).toHaveTitle('Home - beacon');
  await expect(page.getByRole('heading', { name: 'Data flow' })).toBeVisible();
  await expect(page.getByText('Add your first source')).toBeVisible();

  await page.getByRole('link', { name: 'Add a source', exact: true }).click();
  await expect(page).toHaveURL(/\/ui\/sources\/new$/);
  await expect(page).toHaveTitle('Sources - beacon');
  await expect(page.getByRole('heading', { name: 'Add source' })).toBeVisible();

  await page.locator('#src-id').fill(sourceID);
  await page.locator('#src-name').fill(sourceName);
  await page.locator('#src-interface').fill('can0');
  await page.getByRole('button', { name: 'Save' }).click();

  await expect(page).toHaveURL(/\/ui\/dashboard$/);
  await expect(page.getByRole('status')).toContainText(`Source "${sourceID}" created`);
  const sourceLink = page.getByRole('link', { name: sourceName });
  await expect(sourceLink).toBeVisible();
  const sourceRow = sourceLink.locator('xpath=ancestor::tr');
  await expect(sourceRow).toHaveClass(/endpoint-status-row state-(up|degraded|error|restarting|disabled)/);
  expect(await sourceRow.evaluate((row) => getComputedStyle(row).backgroundColor)).not.toBe('rgba(0, 0, 0, 0)');
});

test('connector CEL editor provides autocomplete and live diagnostics', async ({ page }) => {
  await page.goto('/ui/connectors/new');

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

test('MCP reference is available from the header and stays local', async ({ page }) => {
  const requestedOrigins = new Set<string>();
  page.on('request', (request) => requestedOrigins.add(new URL(request.url()).origin));

  await page.goto('/ui/dashboard');
  const mcpLink = page.getByRole('navigation', { name: 'Primary' }).getByRole('link', { name: 'mcp', exact: true });
  await expect(mcpLink).toBeVisible();
  await mcpLink.click();

  await expect(page).toHaveURL(/\/ui\/mcp$/);
  await expect(page).toHaveTitle('MCP - beacon');
  await expect(page.getByRole('heading', { name: 'Model Context Protocol' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Install Beacon MCP' })).toBeVisible();
  await expect(page.getByText('codex mcp add beacon --url http://127.0.0.1:2112/mcp', { exact: true })).toBeVisible();
  await expect(page.getByText('claude mcp add --transport http beacon http://127.0.0.1:2112/mcp', { exact: true })).toBeVisible();
  await expect(page.getByText('gemini mcp add beacon http://127.0.0.1:2112/mcp --transport http', { exact: true })).toBeVisible();
  await expect(page.getByText('.vscode/mcp.json', { exact: true })).toBeVisible();
  await expect(page.getByText('/mcp', { exact: true }).first()).toBeVisible();
  await expect(page.getByRole('table').getByText('get_delivery_statistics', { exact: true })).toBeVisible();
  await expect(page.getByRole('table').getByText('get_source_metrics', { exact: true })).toBeVisible();
  await expect(page.getByRole('table').getByText('commit_source_traffic_baseline', { exact: true })).toBeVisible();
  await expect(page.getByText('offline ready', { exact: true })).toBeVisible();
  expect([...requestedOrigins]).toEqual(['http://127.0.0.1:32112']);
});

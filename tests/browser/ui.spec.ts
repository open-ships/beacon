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
  await expect(page.getByRole('link', { name: sourceName })).toBeVisible();
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

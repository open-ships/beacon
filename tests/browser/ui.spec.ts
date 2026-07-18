import { expect, test } from '@playwright/test';

test('operator can load the dashboard and create a source', async ({ page }) => {
  const sourceID = `browser-test-${Date.now()}`;
  const sourceName = `Playwright source ${sourceID}`;

  await page.goto('/');

  await expect(page).toHaveURL(/\/ui\/dashboard$/);
  await expect(page).toHaveTitle('Dashboard — beacon');
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible();
  await expect(page.getByText('Add your first source')).toBeVisible();

  await page.getByRole('link', { name: 'Sources', exact: true }).click();
  await expect(page).toHaveURL(/\/ui\/sources$/);
  await page.getByRole('button', { name: 'Add source' }).click();
  await expect(page.getByRole('heading', { name: 'Add source' })).toBeVisible();

  await page.locator('#src-id').fill(sourceID);
  await page.locator('#src-name').fill(sourceName);
  await page.locator('#src-interface').fill('can0');
  await page.getByRole('button', { name: 'Save' }).click();

  const sourceRow = page.getByRole('row').filter({ hasText: sourceName });
  await expect(sourceRow).toContainText(sourceID);
  await expect(sourceRow).toContainText('disabled');
});

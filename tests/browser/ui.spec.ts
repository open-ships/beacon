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

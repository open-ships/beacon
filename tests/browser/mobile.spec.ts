import { expect, test } from '@playwright/test';

test('operator pages and validation dialogs fit a touch viewport', async ({ page }) => {
  for (const path of ['/dashboard', '/sources', '/sinks', '/connectors', '/config', '/docs']) {
    expect((await page.goto(path))?.ok()).toBeTruthy();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
  }
  await page.goto('/sources');
  await page.getByRole('link', { name: 'Add source', exact: true }).tap();
  const dialog = page.getByRole('dialog', { name: 'Add source' });
  await dialog.getByLabel('Name', { exact: true }).fill('x'.repeat(257));
  await dialog.getByLabel('Interface', { exact: true }).fill('can0');
  await dialog.getByRole('button', { name: 'Save', exact: true }).tap();
  await expect(dialog.getByRole('alert').filter({ visible: true })).toContainText('name');
  await expect.poll(async () => {
    const box = await dialog.locator('.entity-create-dialog-form').boundingBox();
    return !!box && box.x >= 0 && box.x + box.width <= page.viewportSize()!.width;
  }).toBeTruthy();
  await dialog.getByRole('button', { name: 'Cancel', exact: true }).tap();
  await expect(dialog).toHaveCount(0);
});

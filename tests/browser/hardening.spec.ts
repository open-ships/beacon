import { expect, test as base, type Locator } from '@playwright/test';

// Each test owns its fixtures; leave the shared in-memory appliance empty for
// the existing first-run journey and never depend on browser test ordering.
const test = base.extend<{ entities: { track: (kind: string, id: string) => void; seed: () => Promise<{ source: string; sink: string }> } }>({
  entities: async ({ request }, use) => {
    const ids: { kind: string; id: string }[] = [];
    const track = (kind: string, id: string) => ids.push({ kind, id });
    await use({ track, seed: async () => {
      const prefix = `hardening-${Date.now()}`;
      const source = `${prefix}-source`, sink = `${prefix}-sink`;
      for (const [kind, data] of [
        ['sources', { id: source, name: 'Fixture source', type: 'socketcan', interface: 'can0', enabled: false }],
        ['sinks', { id: sink, name: 'Fixture sink', type: 'null', enabled: false }],
      ] as const) {
        expect((await request.put(`/api/v1/${kind}/${data.id}`, { data })).ok()).toBeTruthy();
        track(kind, data.id);
      }
      return { source, sink };
    }});
    for (const { kind, id } of ids.sort((a, b) => Number(b.kind === 'connectors') - Number(a.kind === 'connectors'))) {
      expect((await request.delete(`/api/v1/${kind}/${id}`)).ok()).toBeTruthy();
    }
  },
});

async function assertControlNames(form: Locator) {
  for (const control of await form.locator('input:not([type=hidden]), select, textarea').all()) {
    await expect(control).toHaveAccessibleName(/\S/);
  }
}

test('every transport field and configuration control has an accessible name', async ({ page }) => {
  for (const kind of ['source', 'sink']) {
    await page.goto(`/${kind}s/new`);
    const form = page.locator('.entity-form');
    const types = await form.getByLabel('Type', { exact: true }).locator('option').evaluateAll(options => options.map(option => (option as HTMLOptionElement).value));
    for (const type of types) {
      const changed = page.waitForResponse(r => r.url().includes(`/frag/${kind}-type-fields`));
      await form.getByLabel('Type', { exact: true }).selectOption(type);
      await changed;
      await assertControlNames(form);
    }
  }
  await page.goto('/config');
  await assertControlNames(page.locator('form'));
});

for (const entry of ['list', 'dashboard']) {
  test(`validation stays in each dialog and replacement buttons close it from ${entry}`, async ({ page, entities }) => {
    const { source, sink } = await entities.seed();
    for (const kind of ['source', 'sink', 'connector']) {
      await page.goto(entry === 'list' ? `/${kind}s` : '/dashboard');
      await page.getByRole('link', { name: entry === 'list' ? `Add ${kind}` : `New ${kind}`, exact: true }).click();
      const dialog = page.getByRole('dialog', { name: `Add ${kind}` });
      await dialog.getByLabel('Name', { exact: true }).fill('x'.repeat(257));
      if (kind === 'connector') {
        await dialog.getByLabel('Source', { exact: true }).selectOption(source);
        await dialog.getByLabel('Sink', { exact: true }).selectOption(sink);
      } else {
        await dialog.getByLabel('Interface', { exact: true }).fill('can0');
      }
      await assertControlNames(dialog);
      await dialog.getByRole('button', { name: 'Save', exact: true }).click();
      await expect(dialog.getByRole('alert').filter({ visible: true })).toContainText('name');
      await expect(dialog.getByLabel('Name', { exact: true })).toHaveValue('x'.repeat(257));
      expect(await page.locator('[id]').evaluateAll(elements => {
        const ids = elements.map(el => el.id);
        return ids.filter((id, i) => ids.indexOf(id) !== i);
      })).toEqual([]);
      if (kind === 'source') await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
      else if (kind === 'sink') await dialog.getByRole('button', { name: 'Close add sink dialog' }).click();
      else await page.keyboard.press('Escape');
      await expect(dialog).toHaveCount(0);
    }
  });
}

test('type changes and SQL previews keep credentials out of URLs', async ({ page }) => {
  const requests: { method: string; url: string; body: string }[] = [];
  page.on('request', r => requests.push({ method: r.method(), url: r.url(), body: r.postData() ?? '' }));
  for (const kind of ['source', 'sink']) {
    await page.goto(`/${kind}s`);
    await page.getByRole('link', { name: `Add ${kind}`, exact: true }).click();
    const dialog = page.getByRole('dialog', { name: `Add ${kind}` });
    await dialog.getByLabel('Type', { exact: true }).selectOption(kind === 'source' ? 'http_sse' : 'http_post');
    await dialog.getByLabel('Headers', { exact: false }).fill('X-API-Key: browser-secret-canary');
    const changed = page.waitForRequest(r => r.url().includes(`/frag/${kind}-type-fields`) && r.postData()?.includes('browser-secret-canary') === true);
    await dialog.getByLabel('Type', { exact: true }).selectOption(kind === 'source' ? 'http_ws' : 'postgres');
    expect((await changed).method()).toBe('POST');
    await expect(dialog.getByLabel('Type', { exact: true })).toHaveValue(kind === 'source' ? 'http_ws' : 'postgres');
    if (kind === 'sink') {
      await dialog.getByLabel('Connection URL', { exact: true }).fill('postgres://operator:browser-secret-canary@localhost/beacon');
      // Wait until htmx has bound the newly swapped controls before typing.
      await expect(dialog.locator('#sink-type-fields')).not.toHaveClass(/htmx-settling/);
      await dialog.getByLabel('Destination table', { exact: true }).fill('public.readings');
      const ddl = page.waitForRequest(r => r.url().includes('/frag/sink-postgres-ddl'));
      await dialog.getByRole('checkbox', { name: 'Automatically create and verify the table' }).uncheck();
      await ddl;
      await expect(dialog.locator('#sink-postgres-ddl-code')).toContainText('"public"."readings"');
    }
    await assertControlNames(dialog);
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  }
  expect(requests.some(r => r.body.includes('browser-secret-canary'))).toBeTruthy();
  expect(requests.filter(r => r.url.includes('browser-secret-canary'))).toEqual([]);
});

for (const failure of ['server', 'network', 'timeout']) {
  test(`failed saves retain entries and expose ${failure} feedback`, async ({ page }) => {
    await page.goto('/sources');
    await page.getByRole('link', { name: 'Add source', exact: true }).click();
    const dialog = page.getByRole('dialog', { name: 'Add source' });
    await dialog.getByLabel('Name', { exact: true }).fill('Keep my edits');
    await dialog.getByLabel('Interface', { exact: true }).fill('can0');
    if (failure === 'timeout') await page.evaluate(() => { (window as any).htmx.config.timeout = 100; });
    await page.route('**/sources', async route => {
      if (route.request().method() !== 'POST') return route.continue();
      if (failure === 'server') return route.fulfill({ status: 500, body: 'internal server error' });
      if (failure === 'network') return route.abort('failed');
      await new Promise(resolve => setTimeout(resolve, 350));
      await route.fulfill({ status: 500, body: 'internal server error' });
    });
    await dialog.getByRole('button', { name: 'Save', exact: true }).click();
    const feedback = dialog.getByRole('alert').filter({ visible: true });
    await expect(feedback).toContainText('Save could not be confirmed');
    await expect(feedback).toBeFocused();
    await expect(dialog.getByLabel('Name', { exact: true })).toHaveValue('Keep my edits');
    await expect(dialog.getByRole('button', { name: 'Save', exact: true })).toBeEnabled();
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
    await expect(dialog).toHaveCount(0);
  });
}

test('failed polling marks displayed status stale and clears on recovery', async ({ page }) => {
  let failed = true;
  await page.route('**/frag/dashboard*', route => failed ? route.fulfill({ status: 503, body: 'unavailable' }) : route.continue());
  await page.goto('/dashboard');
  await expect(page.locator('#request-feedback')).toContainText('Values may be out of date', { timeout: 10_000 });
  failed = false;
  await expect(page.locator('#request-feedback')).toBeHidden({ timeout: 10_000 });
});

test('closing connector dialogs disposes document listeners', async ({ page, entities }) => {
  await entities.seed();
  await page.addInitScript(() => {
    const listeners = new Set<EventListenerOrEventListenerObject>();
    const add = document.addEventListener.bind(document), remove = document.removeEventListener.bind(document);
    document.addEventListener = ((type: string, listener: EventListenerOrEventListenerObject, options: any) => {
      if (type === 'pointerdown') listeners.add(listener);
      add(type, listener, options);
    }) as typeof document.addEventListener;
    document.removeEventListener = ((type: string, listener: EventListenerOrEventListenerObject, options: any) => {
      if (type === 'pointerdown') listeners.delete(listener);
      remove(type, listener, options);
    }) as typeof document.removeEventListener;
    (window as any).documentPointerListeners = listeners;
  });
  await page.goto('/connectors');
  const count = () => page.evaluate(() => (window as any).documentPointerListeners.size);
  const baseline = await count();
  for (let i = 0; i < 5; i++) {
    await page.getByRole('link', { name: 'Add connector', exact: true }).click();
    const dialog = page.getByRole('dialog', { name: 'Add connector' });
    await expect(dialog.locator('[data-cel-autocomplete]')).toBeVisible();
    expect(await count()).toBe(baseline + 1);
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
    await expect(dialog).toHaveCount(0);
    expect(await count()).toBe(baseline);
  }
});

test('direct edit pages return to the saved overview with JavaScript', async ({ page, request, entities }) => {
  const { source, sink } = await entities.seed();
  const connector = `${source}-route`;
  expect((await request.put(`/api/v1/connectors/${connector}`, { data: { id: connector, name: 'Fixture route', source_id: source, sink_id: sink, enabled: false, buffer: {} } })).ok()).toBeTruthy();
  entities.track('connectors', connector);
  for (const [kind, id] of [['sources', source], ['sinks', sink], ['connectors', connector]]) {
    await page.goto(`/${kind}/${id}/edit`);
    await expect(page.locator('dialog')).toHaveCount(0);
    await page.getByLabel('Name', { exact: true }).fill(`Saved ${kind}`);
    await page.getByRole('button', { name: 'Save', exact: true }).click();
    await expect(page).toHaveURL(new RegExp(`/${kind}/${id}/$`));
    await expect(page.getByRole('heading', { name: `Saved ${kind}`, exact: true })).toBeVisible();
  }
});

test('configuration create, validation, type changes and edits work without JavaScript', async ({ browser, request, entities }) => {
  const context = await browser.newContext({ javaScriptEnabled: false });
  const page = await context.newPage();
  const ids: Record<string, string> = {};
  try {
    for (const kind of ['source', 'sink', 'connector']) {
      await page.goto(`/${kind}s/new`);
      const form = page.locator('.entity-form');
      ids[kind] = await form.locator('input[name=id]').inputValue();
      await form.getByLabel('Enabled', { exact: true }).uncheck();
      await form.getByLabel('Name', { exact: true }).fill(`Native ${kind}`);
      if (kind === 'source' || kind === 'sink') {
        await form.getByLabel('Type', { exact: true }).selectOption(kind === 'source' ? 'http_sse' : 'null');
        await form.getByRole('button', { name: 'Update type fields' }).click();
        await expect(form.getByLabel('Name', { exact: true })).toHaveValue(`Native ${kind}`);
        expect((await request.get(`/api/v1/${kind}s/${ids[kind]}`)).status()).toBe(404);
        if (kind === 'source') await form.getByLabel('URL', { exact: true }).fill('https://example.com/events');
      } else {
        await form.getByLabel('Source', { exact: true }).selectOption(ids.source);
        await form.getByLabel('Sink', { exact: true }).selectOption(ids.sink);
        await form.getByLabel('Max age', { exact: true }).fill('invalid');
        // Exercise native keyboard submission on the long connector form.
        await form.getByRole('button', { name: 'Save', exact: true }).press('Enter');
        await expect(form.getByRole('alert').filter({ visible: true })).toContainText('duration');
        await expect(page.getByRole('navigation').first()).toBeVisible();
        await form.getByLabel('Max age', { exact: true }).fill('24h');
      }
      await form.getByRole('button', { name: 'Save', exact: true }).press('Enter');
      await expect(page).toHaveURL(/\/dashboard$/);
      entities.track(`${kind}s`, ids[kind]);
      await page.goto(`/${kind}s/${ids[kind]}/edit`);
      await page.getByLabel('Name', { exact: true }).fill(`Edited native ${kind}`);
      await page.getByRole('button', { name: 'Save', exact: true }).press('Enter');
      await expect(page).toHaveURL(new RegExp(`/${kind}s/${ids[kind]}/$`));
      await expect(page.getByRole('heading', { name: `Edited native ${kind}`, exact: true })).toBeVisible();
      const saved = await (await request.get(`/api/v1/${kind}s/${ids[kind]}`)).json();
      expect(saved.name).toBe(`Edited native ${kind}`);
      expect(saved.enabled).toBe(false);
      expect(page.url()).not.toContain('?');
    }
  } finally { await context.close().catch(() => {}); }
});

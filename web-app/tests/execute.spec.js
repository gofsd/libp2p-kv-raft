// Real end-to-end coverage for MainHandle's pollExecute and the
// watchExecute/watchCommandLog background-loop plumbing. Doesn't attempt
// a genuine cross-peer Execute delivery (that needs a second real node
// willing to dial this tab back, out of scope for a single-tab spec) --
// covers what a lone tab against a real cluster can verify on its own:
// pollExecute reports nothing pending against an empty queue, and a
// watch loop actually starts and stops (its returned WatchHandle.stop()
// promise resolves) rather than hanging -- the one genuinely new wasm
// pattern this port introduces (Rc<MainHandleInner> cloned into a
// wasm_bindgen_futures::spawn_local background task, see app.rs's
// WatchHandle doc comment).
//
// Needs a `kvnode` already running the same way set_get.spec.js's doc
// comment describes; point this test at it with the same KVNODE_MULTIADDR
// env var. Skipped entirely if unset.
import { test, expect } from "@playwright/test";

const nodeMultiaddr = process.env.KVNODE_MULTIADDR;

test.skip(!nodeMultiaddr, "KVNODE_MULTIADDR not set -- see this file's doc comment");

test("pollExecute reports nothing pending against an empty queue", async ({ page }) => {
  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });

  await page.goto("/");
  await page.waitForFunction(() => window.__kvE2E !== undefined);
  await page.evaluate((addr) => window.__kvE2E.handle.connect(addr), nodeMultiaddr);

  const resultJson = await page.evaluate(() => window.__kvE2E.handle.pollExecute());
  expect(JSON.parse(resultJson)).toEqual({ pending: false });

  expect(consoleErrors, `unexpected console errors: ${consoleErrors.join("\n")}`).toEqual([]);
});

test("watchExecute and watchCommandLog start and stop cleanly", async ({ page }) => {
  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });

  await page.goto("/");
  await page.waitForFunction(() => window.__kvE2E !== undefined);
  await page.evaluate((addr) => window.__kvE2E.handle.connect(addr), nodeMultiaddr);

  // Each stop() await is itself the test: WatchHandle::stop's returned
  // Promise only resolves once the background loop has actually observed
  // the stop flag and exited (see app.rs's WatchHandle doc comment on why
  // that matters) -- a Playwright test timeout here means that plumbing
  // is broken, not a flaky assertion.
  await page.evaluate(() => {
    const watch = window.__kvE2E.handle.watchExecute(() => {});
    return watch.stop();
  });

  await page.evaluate(() => {
    const watch = window.__kvE2E.handle.watchCommandLog("pw-test-instance", () => {});
    return watch.stop();
  });

  expect(consoleErrors, `unexpected console errors: ${consoleErrors.join("\n")}`).toEqual([]);
});

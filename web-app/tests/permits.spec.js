// Real end-to-end coverage for MainHandle's requestPermit/requestLogPermit
// -- the only two permit operations reachable from a non-voting learner
// (confirmPermit/revokePermit and their log-permit counterparts are
// voter-gated server-side and permanently unreachable from this client --
// see client.rs's doc comment). Drives window.__kvE2E.handle directly
// (exposed for exactly this, see main.js's doc comment on that global),
// bypassing the UI -- these aren't operations the page's own buttons
// expose.
//
// Needs a `kvnode` already running the same way set_get.spec.js's doc
// comment describes; point this test at it with the same KVNODE_MULTIADDR
// env var. Skipped entirely if unset.
import { test, expect } from "@playwright/test";

const nodeMultiaddr = process.env.KVNODE_MULTIADDR;

test.skip(!nodeMultiaddr, "KVNODE_MULTIADDR not set -- see this file's doc comment");

test("requestPermit and requestLogPermit both succeed against a real cluster", async ({ page }) => {
  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });

  await page.goto("/");
  await page.waitForFunction(() => window.__kvE2E !== undefined);

  const peerId = await page.evaluate(
    (addr) => window.__kvE2E.handle.connect(addr),
    nodeMultiaddr
  );
  expect(peerId).toMatch(/^12D3Koo/);

  // Any node may lodge a permit request (see EventPermitRequest's doc
  // comment) -- this should succeed even though *confirming* it never
  // could from this client.
  await page.evaluate(
    ({ peerId }) => window.__kvE2E.handle.requestPermit("peer", peerId, "requested by permits.spec.js"),
    { peerId }
  );

  await page.evaluate(
    ({ peerId }) =>
      window.__kvE2E.handle.requestLogPermit("pw-test-log-kind", peerId, "requested by permits.spec.js"),
    { peerId }
  );

  expect(consoleErrors, `unexpected console errors: ${consoleErrors.join("\n")}`).toEqual([]);
});

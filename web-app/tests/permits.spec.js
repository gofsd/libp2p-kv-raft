// Real end-to-end coverage for MainHandle's requestPermit -- the only
// permit operation reachable from a non-voting learner (permitConfirm and
// permitRevoke are voter-gated server-side and permanently unreachable
// from this client -- see client.rs's doc comment). Drives
// window.__kvE2E.handle directly (exposed for exactly this, see main.js's
// doc comment on that global), bypassing the UI -- these aren't operations
// the page's own buttons expose.
//
// There is no log-permit counterpart any more: the separate
// requestLogPermit/confirmLogPermit/revokeLogPermit lifecycle (and the
// "peer" permit kind this spec used to request) were removed from the wire
// protocol outright, and log-append authorization now runs through the
// Group/Command ACL catalog instead.
//
// Needs a `kvnode` already running the same way set_get.spec.js's doc
// comment describes; point this test at it with the same KVNODE_MULTIADDR
// env var. Skipped entirely if unset.
import { test, expect } from "@playwright/test";

const nodeMultiaddr = process.env.KVNODE_MULTIADDR;

test.skip(!nodeMultiaddr, "KVNODE_MULTIADDR not set -- see this file's doc comment");

test("requestPermit succeeds against a real cluster", async ({ page }) => {
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

  // Any node may lodge a permit request (see permitRequest's doc comment
  // in api/shmevent.capnp) -- this should succeed even though *confirming*
  // it never could from this client. "cluster-join" is a kind a learner
  // genuinely has reason to request; it's also one shmevent::system's
  // kind_from_name still knows, which "peer" no longer is.
  await page.evaluate(
    ({ peerId }) =>
      window.__kvE2E.handle.requestPermit("cluster-join", peerId, "requested by permits.spec.js"),
    { peerId }
  );

  // An unknown kind is rejected client-side, before anything is sent --
  // the guard that would have caught this spec's own stale "peer" kind.
  const rejected = await page.evaluate(({ peerId }) =>
    window.__kvE2E.handle
      .requestPermit("pw-test-not-a-kind", peerId, "should be rejected")
      .then(() => "", (e) => String(e))
  , { peerId });
  expect(rejected).toContain("unknown permit kind");

  expect(consoleErrors, `unexpected console errors: ${consoleErrors.join("\n")}`).toEqual([]);
});

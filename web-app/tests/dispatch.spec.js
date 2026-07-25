// Real end-to-end coverage for MainHandle's generic log-record surface
// (logAppend/logQuery) and the parts of the command-dispatch surface that
// don't require voter-provisioned catalog fixtures (submitCommand against
// an unpermitted/nonexistent command must fail cleanly rather than crash;
// listExecutionsByPeer must return a well-formed array even with no
// history). logAppend/logQuery need no catalog setup at all -- unlike
// Group/Command CRUD, EventLogAppend is not voter-gated (see
// dispatch.rs's doc comment) -- so this is the one full write+read round
// trip this spec can drive entirely from the web side.
//
// Needs a `kvnode` already running the same way set_get.spec.js's doc
// comment describes; point this test at it with the same KVNODE_MULTIADDR
// env var. Skipped entirely if unset.
import { test, expect } from "@playwright/test";

const nodeMultiaddr = process.env.KVNODE_MULTIADDR;

test.skip(!nodeMultiaddr, "KVNODE_MULTIADDR not set -- see this file's doc comment");

test("logAppend/logQuery round trip and dispatch reads work against a real cluster", async ({ page }) => {
  // Generous: a debug run against this spec's real shared target measured
  // the just-appended record taking up to ~6s to become locally readable
  // (ordinary raft replication lag over a real cross-continental link, not
  // this test's own overhead), and that was on a good run -- the retry
  // budget below needs real headroom past that, not just past the median.
  test.setTimeout(120_000);

  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });

  await page.goto("/");
  await page.waitForFunction(() => window.__kvE2E !== undefined);

  const peerId = await page.evaluate((addr) => window.__kvE2E.handle.connect(addr), nodeMultiaddr);

  const kind = "pw-test-log";
  const unitId = `pw-test-unit-${Date.now()}`;
  const fieldsJson = JSON.stringify({ hello: "world" });
  const narrative = "written by dispatch.spec.js";

  await page.evaluate(
    ({ kind, unitId, fieldsJson, narrative }) =>
      window.__kvE2E.handle.logAppend(kind, unitId, fieldsJson, narrative),
    { kind, unitId, fieldsJson, narrative }
  );

  // The just-written record may lag behind this tab's own local read --
  // same caveat app.rs's do_get documents in detail: this spec's shared
  // e2e leader's raft log keeps growing across every past e2e run ever
  // recorded against it, so a tab that only just joined starts catching
  // up from index 1, not from "recent" -- do_get's own GET_RETRY_BUDGET_MS
  // is 30s for exactly this reason, so match it here rather than the
  // shorter budget set_get.spec.js's Get-after-Set retry uses (that spec
  // runs against a possibly-different, less log-heavy target).
  await expect(async () => {
    const recordsJson = await page.evaluate(
      ({ kind, unitId }) => window.__kvE2E.handle.logQuery(kind, unitId, "", "", ""),
      { kind, unitId }
    );
    const records = JSON.parse(recordsJson);
    expect(records).toHaveLength(1);
    expect(records[0].narrative).toBe(narrative);
    expect(records[0].fields).toEqual({ hello: "world" });
    expect(records[0].author_peer_id).toBe(peerId);
  }).toPass({ timeout: 60_000 });

  // submitCommand against a nonexistent command must fail cleanly (this
  // tab is never permitted for a command that doesn't exist), not hang or
  // crash the Worker.
  await expect(
    page.evaluate(
      (id) => window.__kvE2E.handle.submitCommand(id, "{}"),
      `pw-test-nonexistent-command-${Date.now()}`
    )
  ).rejects.toThrow();

  const executionsJson = await page.evaluate(
    (peerId) => window.__kvE2E.handle.listExecutionsByPeer(peerId),
    peerId
  );
  expect(Array.isArray(JSON.parse(executionsJson))).toBe(true);

  expect(consoleErrors, `unexpected console errors: ${consoleErrors.join("\n")}`).toEqual([]);
});

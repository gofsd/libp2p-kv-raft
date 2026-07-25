// Real end-to-end coverage for MainHandle's catalog *read* surface
// (getGroup/listGroups/getCommand/listCommands/listGroupsForCommand/
// listGroupsForPeer) -- create/update/delete/addXToY are voter-gated
// server-side and permanently unreachable from this client (see
// catalog.rs's doc comment), so there's no way for this spec to
// provision its own fixture data the way a desktop/mobile test could
// (via `mage creategroup`/`addpeertogroup`). This only exercises the
// read path itself: listing returns a well-formed (possibly empty) JSON
// array, and looking up a definitely-nonexistent id fails cleanly rather
// than hanging or crashing the Worker.
//
// Needs a `kvnode` already running the same way set_get.spec.js's doc
// comment describes; point this test at it with the same KVNODE_MULTIADDR
// env var. Skipped entirely if unset.
import { test, expect } from "@playwright/test";

const nodeMultiaddr = process.env.KVNODE_MULTIADDR;

test.skip(!nodeMultiaddr, "KVNODE_MULTIADDR not set -- see this file's doc comment");

test("catalog reads (listGroups/listCommands/getGroup/getCommand) work against a real cluster", async ({
  page,
}) => {
  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });

  await page.goto("/");
  await page.waitForFunction(() => window.__kvE2E !== undefined);

  await page.evaluate((addr) => window.__kvE2E.handle.connect(addr), nodeMultiaddr);

  const groupsJson = await page.evaluate(() => window.__kvE2E.handle.listGroups());
  expect(Array.isArray(JSON.parse(groupsJson))).toBe(true);

  const commandsJson = await page.evaluate(() => window.__kvE2E.handle.listCommands());
  expect(Array.isArray(JSON.parse(commandsJson))).toBe(true);

  const missingId = `pw-test-nonexistent-${Date.now()}`;
  await expect(page.evaluate((id) => window.__kvE2E.handle.getGroup(id), missingId)).rejects.toThrow();
  await expect(page.evaluate((id) => window.__kvE2E.handle.getCommand(id), missingId)).rejects.toThrow();

  const emptyGroupsForCommand = await page.evaluate(
    (id) => window.__kvE2E.handle.listGroupsForCommand(id),
    missingId
  );
  expect(JSON.parse(emptyGroupsForCommand)).toEqual([]);

  expect(consoleErrors, `unexpected console errors: ${consoleErrors.join("\n")}`).toEqual([]);
});

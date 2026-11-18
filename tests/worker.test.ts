import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { test } from "node:test";
import { doMapTask, doReduceTask, ihash, loadPlugin, parseArgs } from "../src/worker.ts";

test("parseArgs and ihash are deterministic", () => {
  const args = parseArgs(["--coord=http://localhost:9000", "--plugin=./p.ts"]);
  assert.deepEqual(args, { coordUrl: "http://localhost:9000", pluginPath: "./p.ts" });

  const bucket = ihash("alpha", 4);
  assert.equal(bucket >= 0 && bucket < 4, true);
  assert.equal(ihash("alpha", 4), bucket);
});

test("doMapTask and doReduceTask produce expected mr-out files", async () => {
  const prevCwd = process.cwd();
  const dir = await fsp.mkdtemp(join(tmpdir(), "mr-worker-test-"));
  const plugin = await loadPlugin(resolve(prevCwd, "plugins/wc.ts"));

  try {
    process.chdir(dir);
    await fsp.writeFile(join(dir, "in.txt"), "foo bar foo\n");

    await doMapTask(0, "in.txt", 2, "w1", plugin.map);
    await doReduceTask(0, "w1", plugin.reduce);
    await doReduceTask(1, "w1", plugin.reduce);

    const out0 = await fsp.readFile(join(dir, "mr-out-0"), "utf8");
    const out1 = await fsp.readFile(join(dir, "mr-out-1"), "utf8");
    const lines = (out0 + out1).trim().split("\n").sort();

    assert.deepEqual(lines, ["bar 1", "foo 2"]);
  } finally {
    process.chdir(prevCwd);
    await fsp.rm(dir, { recursive: true, force: true });
  }
});

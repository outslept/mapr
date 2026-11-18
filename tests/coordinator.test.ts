import assert from "node:assert/strict";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { createServer } from "../src/coordinator.ts";
import type { PollResponse } from "../src/protocol.ts";

const post = async (baseUrl: string, path: string, body: unknown) => {
  const res = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: res.status, data: (await res.json()) as PollResponse };
};

test("coordinator transitions map -> sleep -> reduce -> done and ignores stale workers", async () => {
  const server = createServer({
    port: 0,
    nReduce: 1,
    inputFiles: ["a.txt"],
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address() as AddressInfo;
  const url = `http://127.0.0.1:${port}`;

  try {
    const mapPoll = await post(url, "/pollTask", { workerId: "w1" });
    assert.deepEqual(mapPoll.data, {
      type: "map",
      mapId: 0,
      file: "a.txt",
      nReduce: 1,
    });

    const sleepPoll = await post(url, "/pollTask", { workerId: "w2" });
    assert.deepEqual(sleepPoll.data, { type: "sleep" });

    await post(url, "/reportTask", {
      workerId: "w2",
      type: "map",
      mapId: 0,
      success: true,
    });
    const stillSleeping = await post(url, "/pollTask", { workerId: "w2" });
    assert.deepEqual(stillSleeping.data, { type: "sleep" });

    await post(url, "/reportTask", {
      workerId: "w1",
      type: "map",
      mapId: 0,
      success: true,
    });

    const reducePoll = await post(url, "/pollTask", { workerId: "w2" });
    assert.deepEqual(reducePoll.data, {
      type: "reduce",
      reduceId: 0,
      nReduce: 1,
    });

    await post(url, "/reportTask", {
      workerId: "w2",
      type: "reduce",
      reduceId: 0,
      success: true,
    });

    const donePoll = await post(url, "/pollTask", { workerId: "w1" });
    assert.deepEqual(donePoll.data, { type: "done" });
  } finally {
    server.close();
    await once(server, "close");
  }
});

test("coordinator reaps timed-out tasks without sleeping", async () => {
  const server = createServer(
    { port: 0, nReduce: 1, inputFiles: ["a.txt"] },
    0,
  );
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address() as AddressInfo;
  const url = `http://127.0.0.1:${port}`;

  try {
    const first = await post(url, "/pollTask", { workerId: "w1" });
    assert.equal(first.data.type, "map");

    const reassigned = await post(url, "/pollTask", { workerId: "w2" });
    assert.equal(reassigned.data.type, "map");
  } finally {
    server.close();
    await once(server, "close");
  }
});

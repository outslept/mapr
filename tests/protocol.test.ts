import assert from "node:assert/strict";
import { test } from "node:test";
import { isPollRequest, isPollResponse, isReportRequest } from "../src/protocol.ts";

test("isPollRequest validates workerId", () => {
  assert.equal(isPollRequest({ workerId: "w1" }), true);
  assert.equal(isPollRequest({ workerId: 123 }), false);
  assert.equal(isPollRequest(null), false);
});

test("isReportRequest validates map and reduce reports", () => {
  assert.equal(isReportRequest({ workerId: "w1", type: "map", mapId: 0, success: true }), true);
  assert.equal(
    isReportRequest({ workerId: "w1", type: "reduce", reduceId: 1, success: false }),
    true,
  );
  assert.equal(isReportRequest({ workerId: "w1", type: "map", mapId: -1, success: true }), false);
});

test("isPollResponse validates all response variants", () => {
  assert.equal(isPollResponse({ type: "sleep" }), true);
  assert.equal(isPollResponse({ type: "done" }), true);
  assert.equal(isPollResponse({ type: "map", mapId: 0, file: "a.txt", nReduce: 2 }), true);
  assert.equal(isPollResponse({ type: "reduce", reduceId: 0, nReduce: 2 }), true);
  assert.equal(isPollResponse({ type: "map", mapId: 0 }), false);
});

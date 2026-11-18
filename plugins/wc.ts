import type { KV, MapFn, ReduceFn } from "../src/protocol.ts";

const WORDS = /[^A-Za-zА-Яа-я0-9_]+/g;

export const map: MapFn = (_filename, content) => {
  const out: KV[] = [];
  for (const word of content.split(WORDS)) {
    if (word.length > 0) {
      out.push({ key: word, value: "1" });
    }
  }
  return out;
};

export const reduce: ReduceFn = (_key, values) => {
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    const parsed = Number.parseInt(values[i]!, 10);
    if (Number.isFinite(parsed)) sum += parsed;
  }
  return String(sum);
};

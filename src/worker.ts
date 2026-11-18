import { randomUUID } from "node:crypto";
import { promises as fsp } from "node:fs";
import { isAbsolute, join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import {
  isPollResponse,
  type KV,
  type MapFn,
  type PluginModule,
  type ReduceFn,
} from "./protocol.ts";

interface Args {
  readonly coordUrl: string;
  readonly pluginPath: string;
}

const SLEEP_IDLE_MS = 200;
const SLEEP_FAILURE_MS = 300;
const FNV_OFFSET_BASIS = 0x811c9dc5;
const FNV_PRIME = 0x01000193;

const parseArgs = (argv: readonly string[]): Args => {
  let coordUrl = "http://127.0.0.1:8787";
  let pluginPath = "./plugins/wc.ts";

  for (const arg of argv) {
    if (arg.startsWith("--coord=")) {
      coordUrl = arg.slice("--coord=".length);
    } else if (arg.startsWith("--plugin=")) {
      pluginPath = arg.slice("--plugin=".length);
    }
  }
  return { coordUrl, pluginPath };
};

const ihash = (key: string, nReduce: number): number => {
  let hash = FNV_OFFSET_BASIS;
  for (let i = 0; i < key.length; i++) {
    hash ^= key.charCodeAt(i);
    hash = Math.imul(hash, FNV_PRIME) >>> 0;
  }
  return (hash >>> 0) % nReduce;
};

const postJSON = async (url: string, body: unknown): Promise<unknown> => {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
};

const writeAtomic = async (filePath: string, data: string, workerId: string): Promise<void> => {
  const tmp = `${filePath}.${workerId}.tmp`;
  await fsp.writeFile(tmp, data);
  await fsp.rename(tmp, filePath);
};

const doMapTask = async (
  mapId: number,
  filename: string,
  nReduce: number,
  workerId: string,
  mapFn: MapFn,
): Promise<void> => {
  const content = await fsp.readFile(filename, "utf8");
  const kvs = mapFn(filename, content);
  const buckets: string[] = new Array<string>(nReduce).fill("");

  for (let i = 0; i < kvs.length; i++) {
    const pair = kvs[i]!;
    const idx = ihash(pair.key, nReduce);
    buckets[idx] += `${JSON.stringify(pair)}\n`;
  }

  await Promise.all(
    buckets.map((payload, reduceId) =>
      writeAtomic(`mr-${mapId}-${reduceId}.jsonl`, payload, workerId),
    ),
  );
};

// shortcut: reads each intermediate bucket into memory, switch to streams if single bucket > 200MB
const collectReduceGroups = async (files: readonly string[]): Promise<Map<string, string[]>> => {
  const groups = new Map<string, string[]>();

  for (const filePath of files) {
    let text: string;
    try {
      text = await fsp.readFile(filePath, "utf8");
    } catch {
      continue;
    }

    let start = 0;
    while (start < text.length) {
      let end = text.indexOf("\n", start);
      if (end === -1) end = text.length;
      if (end > start) {
        const obj = JSON.parse(text.slice(start, end)) as Partial<KV>;
        if (typeof obj.key === "string" && typeof obj.value === "string") {
          const list = groups.get(obj.key);
          if (list !== undefined) {
            list.push(obj.value);
          } else {
            groups.set(obj.key, [obj.value]);
          }
        }
      }
      start = end + 1;
    }
  }
  return groups;
};

const doReduceTask = async (
  reduceId: number,
  workerId: string,
  reduceFn: ReduceFn,
): Promise<void> => {
  const suffix = `-${reduceId}.jsonl`;
  const names = await fsp.readdir(process.cwd());
  const files = names.filter((name) => name.startsWith("mr-") && name.endsWith(suffix));

  const groups = await collectReduceGroups(files);
  const keys = [...groups.keys()].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

  let out = "";
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]!;
    out += `${key} ${reduceFn(key, groups.get(key)!)}\n`;
  }
  await writeAtomic(`mr-out-${reduceId}`, out, workerId);
};

const isPluginModule = (v: unknown): v is PluginModule =>
  typeof v === "object" &&
  v !== null &&
  typeof (v as PluginModule).map === "function" &&
  typeof (v as PluginModule).reduce === "function";

const loadPlugin = async (pluginPath: string): Promise<PluginModule> => {
  const abs = isAbsolute(pluginPath) ? pluginPath : join(process.cwd(), pluginPath);
  const mod: Record<string, unknown> = await import(pathToFileURL(abs).href);
  const candidate = mod.default ?? mod;
  if (isPluginModule(candidate)) return candidate;
  throw new Error("plugin must export map(filename, content) and reduce(key, values)");
};

export { doMapTask, doReduceTask, ihash, loadPlugin, parseArgs };

if (import.meta.main) {
  try {
    const { coordUrl, pluginPath } = parseArgs(process.argv.slice(2));
    const workerId = randomUUID();
    const plugin = await loadPlugin(pluginPath);

    console.log(`worker ${workerId} -> ${coordUrl}`);

    for (;;) {
      let payload: unknown;
      try {
        payload = await postJSON(`${coordUrl}/pollTask`, { workerId });
      } catch {
        process.exit(0);
      }

      if (!isPollResponse(payload) || payload.type === "sleep") {
        await delay(SLEEP_IDLE_MS);
        continue;
      }

      if (payload.type === "done") {
        process.exit(0);
      }

      try {
        if (payload.type === "map") {
          // for demo purposes
          // console.log(`[${workerId.slice(0, 6)}] map #${payload.mapId} <- ${payload.file}`);
          await doMapTask(payload.mapId, payload.file, payload.nReduce, workerId, plugin.map);
          await postJSON(`${coordUrl}/reportTask`, {
            workerId,
            type: "map",
            mapId: payload.mapId,
            success: true,
          });
        } else {
          // for demo purposes
          // console.log(`[${workerId.slice(0, 6)}] reduce #${payload.reduceId}`);
          await doReduceTask(payload.reduceId, workerId, plugin.reduce);
          await postJSON(`${coordUrl}/reportTask`, {
            workerId,
            type: "reduce",
            reduceId: payload.reduceId,
            success: true,
          });
        }
      } catch {
        try {
          await postJSON(
            `${coordUrl}/reportTask`,
            payload.type === "map"
              ? { workerId, type: "map", mapId: payload.mapId, success: false }
              : { workerId, type: "reduce", reduceId: payload.reduceId, success: false },
          );
        } catch {
          // coordinator unreachable; timeout reaper will recover the task
        }
        await delay(SLEEP_FAILURE_MS);
      }
    }
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}

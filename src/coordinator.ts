import { promises as fsp } from "node:fs";
import * as http from "node:http";
import { glob } from "tinyglobby";
import type { PollRequest, PollResponse, ReportRequest } from "./protocol.ts";
import { isPollRequest, isReportRequest } from "./protocol.ts";

type Phase = "map" | "reduce" | "done";

interface Task {
  readonly id: number;
  readonly file: string;
  state: "idle" | "in-progress" | "done";
  workerId: string | undefined;
  startedAtMs: number | undefined;
}

interface Args {
  readonly port: number;
  readonly nReduce: number;
  readonly inputFiles: readonly string[];
}

const TIMEOUT_MS = 10_000;

const makeTask = (id: number, file = ""): Task => ({
  id,
  file,
  state: "idle",
  workerId: undefined,
  startedAtMs: undefined,
});

const allDone = (tasks: readonly Task[]): boolean =>
  tasks.every((t) => t.state === "done");

const reapList = (tasks: readonly Task[], deadline: number): void => {
  for (let i = 0; i < tasks.length; i++) {
    const t = tasks[i]!;
    if (
      t.state === "in-progress" &&
      t.startedAtMs !== undefined &&
      t.startedAtMs <= deadline
    ) {
      t.state = "idle";
      t.workerId = undefined;
      t.startedAtMs = undefined;
    }
  }
};

const claimIdle = (tasks: readonly Task[], workerId: string): Task | undefined => {
  for (let i = 0; i < tasks.length; i++) {
    const t = tasks[i]!;
    if (t.state === "idle") {
      t.state = "in-progress";
      t.workerId = workerId;
      t.startedAtMs = Date.now();
      return t;
    }
  }
  return undefined;
};

const completeTask = (
  tasks: readonly Task[],
  id: number,
  workerId: string,
  success: boolean,
): boolean => {
  const t = tasks[id];
  if (!t || t.state !== "in-progress" || t.workerId !== workerId) {
    return false;
  }
  t.state = success ? "done" : "idle";
  t.workerId = undefined;
  t.startedAtMs = undefined;
  return allDone(tasks);
};

const parseArgs = async (argv: readonly string[]): Promise<Args> => {
  const portFlag = argv.find((a) => a.startsWith("--port=")) ?? "--port=8787";
  const nReduceFlag = argv.find((a) => a.startsWith("--nReduce=")) ?? "--nReduce=4";
  const port = Number(portFlag.slice("--port=".length));
  const nReduce = Number(nReduceFlag.slice("--nReduce=".length));

  if (!Number.isInteger(port) || port < 1) throw new Error("bad --port");
  if (!Number.isInteger(nReduce) || nReduce < 1) throw new Error("bad --nReduce");

  const patterns = argv
    .filter((a) => !a.startsWith("--"))
    .map((p) => p.replace(/\\/g, "/"));

  const candidates = patterns.length > 0 ? await glob(patterns) : [];
  const stats = await Promise.all(
    candidates.map((p) => fsp.stat(p, { throwIfNoEntry: false })),
  );

  const unique = new Set<string>();
  for (let i = 0; i < candidates.length; i++) {
    if (stats[i]?.isFile()) unique.add(candidates[i]!);
  }

  const inputFiles = [...unique].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  if (inputFiles.length === 0) throw new Error("no input files");

  return { port, nReduce, inputFiles };
};

const readJson = async (req: http.IncomingMessage): Promise<unknown> => {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  const body = Buffer.concat(chunks).toString("utf8");
  return body.length === 0 ? {} : JSON.parse(body);
};

const sendJson = (
  res: http.ServerResponse,
  status: number,
  body: string,
): void => {
  res.writeHead(status, { "Content-Type": "application/json" }).end(body);
};

const createServer = (args: Args, timeoutMs = TIMEOUT_MS): http.Server => {
  const mapTasks = args.inputFiles.map((file, id) => makeTask(id, file));
  const reduceTasks = Array.from({ length: args.nReduce }, (_, id) => makeTask(id));
  let phase: Phase = "map";

  const onPoll = (req: PollRequest): PollResponse => {
    const deadline = Date.now() - timeoutMs;
    reapList(mapTasks, deadline);
    reapList(reduceTasks, deadline);

    if (phase === "map") {
      const task = claimIdle(mapTasks, req.workerId);
      if (task) {
        return {
          type: "map",
          mapId: task.id,
          file: task.file,
          nReduce: args.nReduce,
        };
      }
      if (!allDone(mapTasks)) return { type: "sleep" };
      phase = "reduce";
    }

    if (phase === "reduce") {
      const task = claimIdle(reduceTasks, req.workerId);
      if (task) {
        return { type: "reduce", reduceId: task.id, nReduce: args.nReduce };
      }
      if (!allDone(reduceTasks)) return { type: "sleep" };
      phase = "done";
    }

    return { type: "done" };
  };

  const onReport = (req: ReportRequest): void => {
    if (req.type === "map") {
      if (completeTask(mapTasks, req.mapId, req.workerId, req.success)) {
        phase = "reduce";
      }
    } else if (
      completeTask(reduceTasks, req.reduceId, req.workerId, req.success)
    ) {
      phase = "done";
    }
  };

  return http.createServer(async (req, res) => {
    try {
      if (req.method === "POST" && req.url === "/pollTask") {
        const body = await readJson(req);
        if (!isPollRequest(body)) {
          sendJson(res, 400, '{"error":"bad request"}');
          return;
        }
        sendJson(res, 200, JSON.stringify(onPoll(body)));
        return;
      }

      if (req.method === "POST" && req.url === "/reportTask") {
        const body = await readJson(req);
        if (!isReportRequest(body)) {
          sendJson(res, 400, '{"error":"bad request"}');
          return;
        }
        onReport(body);
        sendJson(res, 200, '{"ok":true}');
        return;
      }

      sendJson(res, 404, '{"error":"not found"}');
    } catch {
      sendJson(res, 500, '{"error":"server error"}');
    }
  });
};

export { createServer, parseArgs };

if (import.meta.main) {
  try {
    const args = await parseArgs(process.argv.slice(2));
    createServer(args).listen(args.port, () => {
      console.log(
        `coordinator :${args.port} maps=${args.inputFiles.length} reduces=${args.nReduce}`,
      );
    });
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}

export interface KV {
  readonly key: string;
  readonly value: string;
}

export interface PollRequest {
  readonly workerId: string;
}

export type PollResponse =
  | {
      readonly type: "map";
      readonly mapId: number;
      readonly file: string;
      readonly nReduce: number;
    }
  | {
      readonly type: "reduce";
      readonly reduceId: number;
      readonly nReduce: number;
    }
  | { readonly type: "sleep" }
  | { readonly type: "done" };

export type ReportRequest =
  | {
      readonly workerId: string;
      readonly type: "map";
      readonly mapId: number;
      readonly success: boolean;
    }
  | {
      readonly workerId: string;
      readonly type: "reduce";
      readonly reduceId: number;
      readonly success: boolean;
    };

export type MapFn = (filename: string, content: string) => readonly KV[];
export type ReduceFn = (key: string, values: readonly string[]) => string;

export interface PluginModule {
  readonly map: MapFn;
  readonly reduce: ReduceFn;
}

const isObject = (val: unknown): val is Record<string, unknown> =>
  typeof val === "object" && val !== null;

const isNonNegativeInt = (val: unknown): val is number =>
  typeof val === "number" && Number.isInteger(val) && val >= 0;

export const isPollRequest = (val: unknown): val is PollRequest =>
  isObject(val) && typeof val.workerId === "string";

export const isReportRequest = (val: unknown): val is ReportRequest => {
  if (!isObject(val) || typeof val.workerId !== "string" || typeof val.success !== "boolean") {
    return false;
  }
  if (val.type === "map") return isNonNegativeInt(val.mapId);
  if (val.type === "reduce") return isNonNegativeInt(val.reduceId);
  return false;
};

export const isPollResponse = (val: unknown): val is PollResponse => {
  if (!isObject(val)) return false;
  switch (val.type) {
    case "sleep":
    case "done":
      return true;
    case "map":
      return (
        isNonNegativeInt(val.mapId) && typeof val.file === "string" && isNonNegativeInt(val.nReduce)
      );
    case "reduce":
      return isNonNegativeInt(val.reduceId) && isNonNegativeInt(val.nReduce);
    default:
      return false;
  }
};

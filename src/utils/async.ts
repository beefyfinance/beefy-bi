import { get, isString } from "lodash";
import { LogInfos, mergeLogsInfos, rootLogger } from "./logger";

const logger = rootLogger.child({ module: "utils", component: "async" });

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class ConnectionTimeoutError extends Error {
  constructor(message: string, public readonly previousError?: any) {
    super(message);
  }
}

export function withTimeout<TRes>(fn: () => Promise<TRes>, timeoutMs: number, logInfos: LogInfos) {
  return new Promise<TRes>((resolve, reject) => {
    const timeout = setTimeout(() => {
      logger.info(mergeLogsInfos({ msg: "Timeout", data: { timeoutMs } }, logInfos));
      reject(new ConnectionTimeoutError(`Timeout after ${timeoutMs}ms`));
    }, timeoutMs);
    fn()
      .then((res) => {
        clearTimeout(timeout);
        resolve(res);
      })
      .catch((error) => {
        clearTimeout(timeout);
        reject(error);
      });
  });
}

export function isConnectionTimeoutError(err: any) {
  if (err instanceof ConnectionTimeoutError) {
    return true;
  }
  const msg = get(err, "message", "");
  if (isString(msg) && msg.toLocaleLowerCase().includes("connection terminated")) {
    return true;
  }
  return false;
}

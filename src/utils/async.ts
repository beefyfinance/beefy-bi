import { LogInfos, mergeLogsInfos, rootLogger } from "./logger";

const logger = rootLogger.child({ module: "utils", component: "async" });

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function withTimeout<TRes>(fn: () => Promise<TRes>, timeoutMs: number, logInfos: LogInfos) {
  return new Promise<TRes>((resolve, reject) => {
    const timeout = setTimeout(() => {
      logger.error(mergeLogsInfos({ msg: "Timeout", data: { timeoutMs } }, logInfos));
      reject(new Error(`Timeout after ${timeoutMs}ms`));
    }, timeoutMs);
    fn().then((res) => {
      clearTimeout(timeout);
      resolve(res);
    });
  });
}

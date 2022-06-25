import { logger } from "./logger";

type ExitCallback = () => Promise<any>;
const exitCallbacks: ExitCallback[] = [];

export function onExit(callback: ExitCallback) {
  exitCallbacks.push(callback);
}

let called = false;
async function exitHandler() {
  if (called) {
    return;
  }
  called = true;
  try {
    await Promise.allSettled(exitCallbacks.map((cb) => cb()));
    logger.info(`[PROCESS] All exit handlers done. Bye.`);
    process.exit(0);
  } catch (e) {
    logger.error(`[PROCESS] Exit handlers didn't work properly.`);
    logger.error(e);
    process.exit(1);
  }
}

process.on("SIGTERM", exitHandler);
process.on("SIGINT", exitHandler);

export async function runMain(main: () => Promise<any>) {
  try {
    await main();
    await exitHandler();
    logger.info("[MAIN] Done");
    process.exit(0);
  } catch (e) {
    logger.error("[MAIN] ERROR");
    console.log(e);
    logger.error(e);
    await exitHandler();
    process.exit(1);
  }
}

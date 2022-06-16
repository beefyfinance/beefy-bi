type ExitCallback = () => Promise<any>;
const exitCallbacks: ExitCallback[] = [];

export function onExit(callback: ExitCallback) {
  exitCallbacks.push(callback);
}

let called = false;
process.on("SIGTERM", async () => {
  if (called) {
    return;
  }
  called = true;
  await Promise.allSettled(exitCallbacks.map((cb) => cb()));
});

process.on("SIGINT", async () => {
  if (called) {
    return;
  }
  called = true;
  await Promise.allSettled(exitCallbacks.map((cb) => cb()));
});

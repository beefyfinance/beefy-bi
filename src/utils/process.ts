export function onExit(callback: () => Promise<any>) {
  let called = false;
  process.on("SIGTERM", async () => {
    if (called) {
      return;
    }
    called = true;
    await callback();
  });

  process.on("SIGINT", async () => {
    if (called) {
      return;
    }
    called = true;
    await callback();
  });
}

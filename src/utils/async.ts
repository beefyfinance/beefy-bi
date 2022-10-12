export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function withTimeout<TRes>(fn: () => Promise<TRes>, timeoutMs: number) {
  return new Promise<TRes>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Timeout after ${timeoutMs}ms`));
    }, timeoutMs);
    fn().then((res) => {
      clearTimeout(timeout);
      resolve(res);
    });
  });
}

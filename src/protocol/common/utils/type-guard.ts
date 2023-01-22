export function isNotNull<T>(o: T | null): o is T {
  return o !== null;
}

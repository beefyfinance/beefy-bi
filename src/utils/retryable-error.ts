import { isArchiveNodeNeededError } from "./rpc/archive-node-needed";
import { isProgrammerError } from "./programmer-error";

export function isErrorRetryable(err: any): boolean {
  if (isArchiveNodeNeededError(err)) {
    return false;
  }
  if (isProgrammerError(err)) {
    return false;
  }
  return true;
}

import { Chain } from "../../types/chain";
import { get, isObjectLike, isString, sample } from "lodash";

export class ArchiveNodeNeededError extends Error {
  constructor(public readonly chain: Chain, public readonly error: any) {
    super(`Archive node needed for ${chain}`);
  }
}
export function isErrorDueToMissingDataFromNode(error: any) {
  // parse from ehter-wrapped rpc calls
  const errorRpcBody = get(error, "error.body");
  if (errorRpcBody && isString(errorRpcBody)) {
    const rpcBodyError = JSON.parse(errorRpcBody);
    const errorCode = get(rpcBodyError, "error.code");
    const errorMessage = get(rpcBodyError, "error.message");

    if (
      errorCode === -32000 &&
      isString(errorMessage) &&
      (errorMessage.includes("Run with --pruning=archive") ||
        // Cf: https://github.com/ethereum/go-ethereum/issues/20557
        errorMessage.startsWith("missing trie node"))
    ) {
      return true;
    }
  }

  // also parse from direct rpc responses
  const directRpcError = get(error, "error");
  if (
    directRpcError &&
    isObjectLike(directRpcError) &&
    get(directRpcError, "code") === -32000 &&
    get(directRpcError, "message")?.startsWith("missing trie node")
  ) {
    return true;
  }
  return false;
}

export function shouldRetryRpcError(error: any) {
  // missing data can't be retried
  return !isErrorDueToMissingDataFromNode(error);
}
import { get, isObjectLike, isString } from "lodash";
import { Chain } from "../../types/chain";

export class ArchiveNodeNeededError extends Error {
  constructor(public readonly chain: Chain, public readonly error: any) {
    super(`Archive node needed for ${chain}`);
  }
}
export function isErrorDueToMissingDataFromNode(error: any) {
  // parse from ehter-wrapped rpc calls
  const errorRpcBody = get(error, "error.body");
  if (errorRpcBody && isString(errorRpcBody)) {
    const [successfulParse, rpcBodyError] = parseJSON(errorRpcBody);
    if (!successfulParse) {
      return false;
    }
    const errorCode = get(rpcBodyError, "error.code");
    const errorMessage = get(rpcBodyError, "error.message");

    if (
      (errorCode === -32000 &&
        isString(errorMessage) &&
        (errorMessage.includes("Run with --pruning=archive") ||
          // Cf: https://github.com/ethereum/go-ethereum/issues/20557
          errorMessage.startsWith("missing trie node"))) ||
      (errorCode === 0 && isString(errorMessage) && errorMessage.includes("we can't execute this request"))
    ) {
      return true;
    }
  }

  // also parse a string content
  if (isString(error)) {
    const [successfulParse, parsedErrorContent] = parseJSON(error);
    if (!successfulParse) {
      return false;
    }
    error = parsedErrorContent;
  }

  // also parse from direct rpc responses
  const directRpcError = get(error, "error");
  const errorCode = get(directRpcError, "code");
  const errorMessage = get(directRpcError, "message");
  if (
    (directRpcError &&
      isObjectLike(directRpcError) &&
      errorCode === -32000 &&
      (errorMessage?.startsWith("missing trie node") || errorMessage?.startsWith("header not found"))) ||
    (errorCode === 0 && isString(errorMessage) && errorMessage?.includes("we can't execute this request"))
  ) {
    return true;
  }
  return false;
}

export function isArchiveNodeNeededError(error: any) {
  if (error instanceof ArchiveNodeNeededError) {
    return true;
  }
  if (isErrorDueToMissingDataFromNode(error)) {
    return true;
  }
  return false;
}

function parseJSON(json: string): any {
  try {
    return [true, JSON.parse(json)];
  } catch (e) {
    return [false, null];
  }
}

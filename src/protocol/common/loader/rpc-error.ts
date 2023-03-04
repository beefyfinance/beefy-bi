import { Chain } from "../../../types/chain";
import { db_query } from "../../../utils/db";
import { removeSecretsFromRpcUrl } from "../../../utils/rpc/remove-secrets-from-rpc-url";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

export interface DbRpcError {
  chain: Chain;
  datetime: Date;
  rpc_url: string;
  request: object;
  response: object | string;
}

export function insertRpcError$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends DbRpcError>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getRpcErrorData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, error: DbRpcError) => TRes;
}) {
  return dbBatchCall$({
    // those errors can be quite big, so we need to limit the number of errors we insert at once
    ctx: {
      ...options.ctx,
      streamConfig: { ...options.ctx.streamConfig, dbMaxInputTake: Math.max(options.ctx.behaviour.dbBatch.maxInputTake / 10, 1) },
    },
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getRpcErrorData,
    logInfos: { msg: "rpc error insert" },
    processBatch: async (objAndData) => {
      await db_query(
        `INSERT INTO rpc_error_ts (chain, datetime, rpc_url, request, response) VALUES %L`,
        [
          objAndData.map(({ data }) => [
            data.chain,
            data.datetime.toISOString(),
            removeSecretsFromRpcUrl(options.ctx.chain, data.rpc_url),
            JSON.stringify(data.request),
            JSON.stringify(data.response),
          ]),
        ],
        options.ctx.client,
      );

      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}

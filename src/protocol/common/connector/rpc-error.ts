import { ethers } from "ethers";
import { get, isArray } from "lodash";
import * as Rx from "rxjs";
import { EthersProviderDebugEvent } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { isEmptyVaultPPFSError } from "../../beefy/connector/ppfs";
import { DbRpcError, insertRpcError$ } from "../loader/rpc-error";
import { ImportCtx } from "../types/import-context";

const logger = rootLogger.child({ module: "common", component: "rpc-error" });

export function saveRpcErrorToDb(options: { ctx: ImportCtx; rpc: ethers.providers.JsonRpcProvider }) {
  const { observable, next, complete } = createObservableWithNext<DbRpcError>();

  options.rpc.on("debug", (event: EthersProviderDebugEvent) => {
    const error = get(event, "error") as any;
    if (event.action === "response" && error) {
      const info: DbRpcError = {
        datetime: new Date(),
        chain: options.ctx.chain,
        rpc_url: options.rpc.connection.url,
        request: get(error, "requestBody") || event.request,
        response: get(error, "body") || error,
      };

      // remove null values
      if (isArray(info.response)) {
        info.response = info.response.filter((x) => x !== null);
      }

      // ignore those errors, we expect they will happen
      if (isArray(info.response)) {
        info.response = info.response.filter((x) => !isEmptyVaultPPFSError(x));
      } else if (isEmptyVaultPPFSError(info.response)) {
        return;
      }
      return next(info);
    }
  });

  // immediately subscribe to the observable so it starts
  observable
    .pipe(
      Rx.tap((err) => logger.trace({ msg: "Got rpc error to insert", data: { err } })),
      insertRpcError$({
        ctx: options.ctx,
        emitError: (obj, report) => {
          logger.error({ msg: "Error inserting rpc error", data: { obj, report } });
        },
        getRpcErrorData: (obj) => obj,
        formatOutput: (obj, err) => ({ obj, err }),
      }),
    )
    // subscribe to is so it starts
    .subscribe(() => {});
  return () => {
    complete();
  };
}

import { ethers } from "ethers";
import { get, isArray } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { DbClient } from "../../../utils/db";
import { EthersProviderDebugEvent } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { DbRpcError, insertRpcError$ } from "../loader/rpc-error";
import { ImportCtx } from "../types/import-context";
import { defaultHistoricalStreamConfig, defaultRecentStreamConfig } from "../utils/rpc-chain-runner";

const logger = rootLogger.child({ module: "common", component: "rpc-error" });

export function saveRpcErrorToDb(options: { client: DbClient; mode: "historical" | "recent"; chain: Chain; rpc: ethers.providers.JsonRpcProvider }) {
  const ctx: ImportCtx = {
    client: options.client,
    chain: options.chain,
    streamConfig: options.mode === "historical" ? defaultHistoricalStreamConfig : defaultRecentStreamConfig,
    rpcConfig: {} as any,
  };

  const { observable, next, complete } = createObservableWithNext<DbRpcError>();

  options.rpc.on("debug", (event: EthersProviderDebugEvent) => {
    const error = get(event, "error") as any;
    if (event.action === "response" && error) {
      const info: DbRpcError = {
        datetime: new Date(),
        chain: options.chain,
        rpc_url: options.rpc.connection.url,
        request: get(error, "requestBody") || event.request,
        response: get(error, "body") || error,
      };

      return next(info);
    }
  });

  // immediately subscribe to the observable so it starts
  observable
    .pipe(
      Rx.tap((err) => logger.trace({ msg: "Got rpc error to insert", data: { err } })),
      insertRpcError$({
        ctx,
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

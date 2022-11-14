import * as Rx from "rxjs";
import { fetchErc20Transfers$ } from "../protocol/common/connector/erc20-transfers";
import { addHistoricalBlockQuery$ } from "../protocol/common/connector/import-queries";
import { createRpcConfig } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../utils/config";
import { DbClient, withPgClient } from "../utils/db";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

async function main(client: DbClient) {
  const chain: Chain = "ethereum";
  const ctx = {
    chain,
    client,
    rpcConfig: createRpcConfig(chain),
    streamConfig: {
      maxInputTake: 500,
      maxInputWaitMs: 1000,
      maxTotalRetryMs: 1000,
      dbMaxInputTake: BATCH_DB_INSERT_SIZE,
      dbMaxInputWaitMs: BATCH_MAX_WAIT_MS,
      workConcurrency: 1,
    },
  };
  const forceCurrentBlockNumber = null;
  const feebatchAddr = "0x8237f3992526036787E8178Def36291Ab94638CD";
  const wtokenAddr = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
  const wtokenDecimals = 18;
  const feebatchCreationBlock = 15897027;
  const feebatchCreationDate = new Date("2022-11-04T15:43:59");
  const emitError = (item: any) => console.error(item);

  const obs$ = Rx.from([{}]).pipe(
    addHistoricalBlockQuery$({
      ctx,
      emitError,
      forceCurrentBlockNumber,
      getFirstBlockNumber: () => feebatchCreationBlock,
      getImport: (item): any => {
        return {
          importData: {
            ranges: {
              coveredRanges: [],
              lastImportDate: null,
              toRetry: [],
            },
          },
        };
      },
      formatOutput: (item, latestBlockNumber, blockQueries) => ({ ...item, latestBlockNumber, blockQueries }),
    }),

    Rx.concatMap((item) => item.blockQueries.map((blockQuery) => ({ ...item, blockQuery }))),

    fetchErc20Transfers$({
      allowFetchingFromEthscan: false,
      ctx,
      emitError,
      getQueryParams: (item) => ({
        address: wtokenAddr,
        decimals: wtokenDecimals,
        fromBlock: item.blockQuery.from,
        toBlock: item.blockQuery.to,
        trackAddress: feebatchAddr,
      }),
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    Rx.concatMap((item) => item.transfers.map((transfer) => ({ blockRange: item.blockQuery, transfer }))),
    Rx.toArray(),
  );
  const res = await consumeObservable(obs$);
  console.log(res);
}

runMain(withPgClient(main, { appName: "beefy:test_script", logInfos: { msg: "test" } }));

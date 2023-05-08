import { cloneDeep } from "lodash";
import * as Rx from "rxjs";
import { fetchProductEvents$ } from "../protocol/beefy/connector/product-events";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { getProductContractAddress } from "../protocol/beefy/utils/contract-accessors";
import { DbBeefyProduct, productList$ } from "../protocol/common/loader/product";
import { ImportBehaviour, ImportCtx, createBatchStreamConfig, defaultImportBehaviour } from "../protocol/common/types/import-context";
import { QueryOptimizerOutput } from "../protocol/common/utils/optimize-range-queries";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { DbClient, withDbClient } from "../utils/db";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

const logger = rootLogger.child({ module: "show-used-rpc-config", component: "main" });

async function main(client: DbClient) {
  const options = {
    mode: "historical",
    chain: "zksync" as Chain,
    disableWorkConcurrency: true,
    //forceRpcUrl: "https://rpc.ankr.com/eth",
    rpcCount: "all",
    ignoreImportState: true,
    skipRecentWindowWhenHistorical: "all", // by default, live products recent data is done by the recent import
  };
  const behaviour: ImportBehaviour = { ...cloneDeep(defaultImportBehaviour), mode: "historical" };
  const rpcConfig = getMultipleRpcConfigsForChain({ chain: options.chain, behaviour })[0];

  const ctx: ImportCtx = {
    chain: options.chain,
    client: {} as any, // unused
    behaviour,
    rpcConfig,
    streamConfig: createBatchStreamConfig(options.chain, behaviour),
  };

  const productAddresses = [
    "0xc2189Fd3fA38F3774c556aaCB5717eE9A7aAEE8a",
    "0x0c3c0cBCc243349525e437D6FfD0F254408c2b4c",
    "0xd3DA44E34E5c57397ea56f368B9e609433eF1d03",
  ].map((a) => a.toLocaleLowerCase());
  const allProducts = await consumeObservable(
    productList$(client, "beefy", options.chain).pipe(
      Rx.filter((product) => product.productData.dashboardEol === false),
      Rx.filter((product) => productAddresses.includes(getProductContractAddress(product).toLocaleLowerCase())),
      Rx.toArray(),
      Rx.map((products) => products.slice(0, 100)),
    ),
  );
  if (allProducts === null || allProducts.length <= 0) {
    throw new Error("no products");
  }

  const range = { from: 3008339, to: 3058340 };
  const result = await consumeObservable(
    Rx.from([
      {
        type: "address batch",
        queries: [
          {
            objs: allProducts.map((product) => ({ product })),
            range,
            postFilters: [],
          },
        ],
      },
      // {
      //   type: "jsonrpc batch",
      //   queries: allProducts.map((product) => ({ obj: { product }, range })),
      // },
    ] as QueryOptimizerOutput<{ product: DbBeefyProduct }, number>[]).pipe(
      fetchProductEvents$({
        ctx,
        emitError: (item, report) => {
          logger.error({ msg: "err", data: { item, report } });
        },
        getCallParams: (query) => query,
        formatOutput: (item, events) => ({ item, events }),
      }),
    ),
  );

  if (result === null) {
    throw new Error("No events");
  }

  console.dir(
    result.events.map((event) => ({
      eventamountTransferred: event.amountTransferred.toString(),
      eventblockNumber: event.blockNumber.toString(),
      eventchain: event.chain.toString(),
      eventlogIndex: event.logIndex.toString(),
      eventlogLineage: event.logLineage.toString(),
      eventownerAddress: event.ownerAddress.toString(),
      eventtokenAddress: event.tokenAddress.toString(),
      eventtokenDecimals: event.tokenDecimals.toString(),
      eventtransactionHash: event.transactionHash.toString(),
    })),
    { depth: null },
  );
}
runMain(withDbClient(main, { appName: "test-ws", logInfos: { msg: "test-ws" } }));

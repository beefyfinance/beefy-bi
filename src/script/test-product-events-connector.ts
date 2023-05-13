import { deepEqual } from "assert";
import { cloneDeep, sortBy } from "lodash";
import * as Rx from "rxjs";
import { fetchProductEvents$ } from "../protocol/beefy/connector/product-events";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { getProductContractAddress } from "../protocol/beefy/utils/contract-accessors";
import {
  isBeefyBoost,
  isBeefyGovVault,
  isBeefyGovVaultOrBoostProductImportQuery,
  isBeefyStandardVaultProductImportQuery,
} from "../protocol/beefy/utils/type-guard";
import { fetchERC20TransferToAStakingContract$, fetchErc20Transfers$ } from "../protocol/common/connector/erc20-transfers";
import { DbBeefyProduct, productList$ } from "../protocol/common/loader/product";
import { ImportBehaviour, ImportCtx, createBatchStreamConfig, defaultImportBehaviour } from "../protocol/common/types/import-context";
import { ImportRangeQuery } from "../protocol/common/types/import-query";
import { QueryOptimizerOutput } from "../protocol/common/utils/optimize-range-queries";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { DbClient, withDbClient } from "../utils/db";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { ProgrammerError } from "../utils/programmer-error";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

const logger = rootLogger.child({ module: "show-used-rpc-config", component: "main" });

async function main(client: DbClient) {
  const options = {
    mode: "historical",
    chain: "bsc" as Chain,
    disableWorkConcurrency: true,
    forceRpcUrl: "https://rpc.ankr.com/bsc",
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

  const range = { from: 27932383 - 10, to: 27932469 + 10 };
  const productAddresses = ["0x32dC019E510bB15b76d12fEDCFF36Dc87ad24C37"].map((a) => a.toLocaleLowerCase());
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
  const emitError = (item: any, report: any) => {
    logger.error({ msg: "err", data: { item, report } });
  };

  const originalPipeline = Rx.pipe(
    // add typings to the input item
    Rx.tap((_: ImportRangeQuery<DbBeefyProduct, number>) => {}),

    // dispatch to all the sub pipelines
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          // set the right product type
          Rx.filter(isBeefyGovVaultOrBoostProductImportQuery),

          fetchERC20TransferToAStakingContract$({
            ctx,
            emitError,
            getQueryParams: (item) => {
              if (isBeefyBoost(item.target)) {
                const boost = item.target.productData.boost;
                return {
                  tokenAddress: boost.staked_token_address,
                  decimals: boost.staked_token_decimals,
                  trackAddress: boost.contract_address,
                  fromBlock: item.range.from,
                  toBlock: item.range.to,
                };
              } else if (isBeefyGovVault(item.target)) {
                // for gov vaults we don't have a share token so we use the underlying token
                // transfers and filter on those transfer from and to the contract address
                const vault = item.target.productData.vault;
                return {
                  tokenAddress: vault.want_address,
                  decimals: vault.want_decimals,
                  trackAddress: vault.contract_address,
                  fromBlock: item.range.from,
                  toBlock: item.range.to,
                };
              } else {
                throw new ProgrammerError({ msg: "Invalid product type, should be gov vault or boost", data: { product: item.target } });
              }
            },
            formatOutput: (item, transfers) => ({
              ...item,
              transfers: transfers.filter((transfer) => transfer.ownerAddress !== transfer.tokenAddress),
            }),
          }),
        ),
        items$.pipe(
          // set the right product type
          Rx.filter(isBeefyStandardVaultProductImportQuery),

          // fetch the vault transfers
          fetchErc20Transfers$({
            ctx,
            emitError,
            getQueryParams: (item) => {
              const vault = item.target.productData.vault;
              return {
                tokenAddress: vault.contract_address,
                decimals: vault.token_decimals,
                fromBlock: item.range.from,
                toBlock: item.range.to,
              };
            },
            formatOutput: (item, transfers) => ({ ...item, transfers }),
          }),
        ),
      ),
    ),
  );

  const addrBatchResults = await consumeObservable(
    Rx.from([
      {
        type: "address batch",
        objs: allProducts.map((product) => ({ product })),
        range,
        postFilters: [],
      },
    ] as QueryOptimizerOutput<{ product: DbBeefyProduct }, number>[]).pipe(
      fetchProductEvents$({
        ctx,
        emitError,
        getCallParams: (query) => query,
        formatOutput: (item, events) => ({ item, events }),
      }),
      Rx.toArray(),
    ),
  );

  const jsonRpcResults = await consumeObservable(
    Rx.from(
      allProducts.map(
        (product) =>
          ({
            type: "jsonrpc batch",
            obj: { product },
            range,
          } as QueryOptimizerOutput<{ product: DbBeefyProduct }, number>),
      ),
    ).pipe(
      fetchProductEvents$({
        ctx,
        emitError,
        getCallParams: (query) => query,
        formatOutput: (item, events) => ({ item, events }),
      }),
      Rx.toArray(),
    ),
  );
  const resultsOriginal = await consumeObservable(
    Rx.from(
      allProducts.map((product) => ({
        latest: range.to + 100,
        range: range,
        target: product,
      })) as ImportRangeQuery<DbBeefyProduct, number>[],
    ).pipe(
      originalPipeline,
      Rx.map((item) => ({ events: item.transfers })),
      Rx.toArray(),
    ),
  );

  if (jsonRpcResults === null || addrBatchResults === null || resultsOriginal === null) {
    throw new Error("No events");
  }

  const originalEvents = sortBy(
    resultsOriginal.flatMap(({ events }) =>
      events.map((event) => ({
        amountTransferred: event.amountTransferred.toString(),
        blockNumber: event.blockNumber.toString(),
        chain: event.chain.toString(),
        //logIndex: event.logIndex.toString(),
        logLineage: event.logLineage.toString(),
        ownerAddress: event.ownerAddress.toString(),
        tokenAddress: event.tokenAddress.toString(),
        tokenDecimals: event.tokenDecimals.toString(),
        transactionHash: event.transactionHash.toString(),
      })),
    ),
    "blockNumber",
  );

  const addrBatchEvents = sortBy(
    addrBatchResults.flatMap(({ events }) =>
      events.map((event) => ({
        amountTransferred: event.amountTransferred.toString(),
        blockNumber: event.blockNumber.toString(),
        chain: event.chain.toString(),
        //logIndex: event.logIndex.toString(),
        logLineage: event.logLineage.toString(),
        ownerAddress: event.ownerAddress.toString(),
        tokenAddress: event.tokenAddress.toString(),
        tokenDecimals: event.tokenDecimals.toString(),
        transactionHash: event.transactionHash.toString(),
      })),
    ),
    "blockNumber",
  );
  const jsonRpcEvents = sortBy(
    jsonRpcResults.flatMap(({ events }) =>
      events.map((event) => ({
        amountTransferred: event.amountTransferred.toString(),
        blockNumber: event.blockNumber.toString(),
        chain: event.chain.toString(),
        //logIndex: event.logIndex.toString(),
        logLineage: event.logLineage.toString(),
        ownerAddress: event.ownerAddress.toString(),
        tokenAddress: event.tokenAddress.toString(),
        tokenDecimals: event.tokenDecimals.toString(),
        transactionHash: event.transactionHash.toString(),
      })),
    ),
    "blockNumber",
  );

  console.dir({ addrBatchEvents, jsonRpcEvents, originalEvents }, { depth: null });
  deepEqual(addrBatchEvents, jsonRpcEvents, "addrBatchEvents === jsonRpcEvents");
  deepEqual(addrBatchEvents, originalEvents, "addrBatchEvents === originalEvents");
  deepEqual(jsonRpcEvents, originalEvents, "jsonRpcEvents === originalEvents");
}

runMain(withDbClient(main, { appName: "test-ws", logInfos: { msg: "test-ws" } }));

import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { DbClient } from "../../../../utils/db";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchContractCreationInfos$ } from "../../../common/connector/contract-creation";
import { addHistoricalBlockQuery$, addLatestBlockQuery$ } from "../../../common/connector/import-queries";
import { upsertBlock$ } from "../../../common/loader/blocks";
import { DbProductInvestmentImportState } from "../../../common/loader/import-state";
import { DbBeefyProduct } from "../../../common/loader/product";
import { createHistoricalImportPipeline, createRecentImportPipeline } from "../../../common/utils/historical-recent-pipeline";
import { getProductContractAddress } from "../../utils/contract-accessors";
import { isBeefyProductLive } from "../../utils/type-guard";
import { importProductBlockRange$ } from "./product-block-range";

export const getImportStateKey = (product: DbBeefyProduct) => `product:investment:${product.productId}`;

export function importChainHistoricalData$(options: { client: DbClient; chain: Chain; forceCurrentBlockNumber: number | null; rpcCount: number }) {
  return createHistoricalImportPipeline<DbBeefyProduct, number, DbProductInvestmentImportState>({
    client: options.client,
    chain: options.chain,
    rpcCount: options.rpcCount,
    logInfos: { msg: "Importing historical beefy investments", data: { chain: options.chain } },
    getImportStateKey,
    isLiveItem: isBeefyProductLive,
    generateQueries$: (ctx, emitError) =>
      Rx.pipe(
        addHistoricalBlockQuery$({
          ctx,
          emitError,
          forceCurrentBlockNumber: options.forceCurrentBlockNumber,
          getImport: (item) => item.importState,
          getFirstBlockNumber: (importState) => importState.importData.contractCreatedAtBlock,
          formatOutput: (item, latestBlockNumber, blockQueries) => blockQueries.map((range) => ({ ...item, range, latest: latestBlockNumber })),
        }),
        Rx.concatAll(),
      ),
    createDefaultImportState$: (ctx) =>
      Rx.pipe(
        // initialize the import state
        // find the contract creation block
        fetchContractCreationInfos$({
          rpcConfig: ctx.rpcConfig,
          getCallParams: (obj) => ({
            chain: ctx.chain,
            contractAddress: getProductContractAddress(obj),
          }),
          formatOutput: (obj, contractCreationInfo) => ({ obj, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        // add this block to our global block list
        upsertBlock$({
          ctx,
          emitError: () => {
            throw new Error("Failed to upsert block");
          },
          getBlockData: (item) => ({
            datetime: item.contractCreationInfo.datetime,
            chain: item.obj.chain,
            blockNumber: item.contractCreationInfo.blockNumber,
            blockData: {},
          }),
          formatOutput: (item, block) => ({ ...item, block }),
        }),

        Rx.map((item) => ({
          obj: item.obj,
          importData: {
            type: "product:investment",
            productId: item.obj.productId,
            chain: item.obj.chain,
            chainLatestBlockNumber: item.contractCreationInfo.blockNumber,
            contractCreatedAtBlock: item.contractCreationInfo.blockNumber,
            contractCreationDate: item.contractCreationInfo.datetime,
            ranges: {
              lastImportDate: new Date(),
              coveredRanges: [],
              toRetry: [],
            },
          },
        })),
      ),
    processImportQuery$: (ctx, emitError) =>
      importProductBlockRange$({ ctx, emitBoostError: emitError, emitGovVaultError: emitError, emitStdVaultError: emitError, mode: "historical" }),
  });
}

export function importChainRecentData$(options: { client: DbClient; chain: Chain; forceCurrentBlockNumber: number | null; rpcCount: number }) {
  return createRecentImportPipeline<DbBeefyProduct, number>({
    client: options.client,
    chain: options.chain,
    rpcCount: options.rpcCount,
    cacheKey: "beefy:product:investment:recent",
    logInfos: { msg: "Importing recent beefy investments", data: { chain: options.chain } },
    getImportStateKey,
    isLiveItem: isBeefyProductLive,
    generateQueries$: ({ ctx, emitError, lastImported, formatOutput }) =>
      addLatestBlockQuery$({
        ctx,
        emitError,
        forceCurrentBlockNumber: options.forceCurrentBlockNumber,
        getLastImportedBlock: () => lastImported,
        formatOutput: (item, latest, range) => formatOutput(item, latest, [range]),
      }),
    processImportQuery$: (ctx, emitError) =>
      importProductBlockRange$({ ctx, emitBoostError: emitError, emitGovVaultError: emitError, emitStdVaultError: emitError, mode: "recent" }),
  });
}

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
import { isBeefyProductLive } from "../../utils/type-guard";
import { importProductBlockRange$ } from "./product-block-range";

const getImportStateKey = (product: DbBeefyProduct) => `product:investment:${product.productId}`;

export function importChainHistoricalData$(client: DbClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  return createHistoricalImportPipeline<DbBeefyProduct, number, DbProductInvestmentImportState>({
    client,
    chain,
    logInfos: { msg: "Importing historical beefy investments", data: { chain } },
    getImportStateKey,
    isLiveItem: isBeefyProductLive,
    generateQueries$: (ctx) =>
      Rx.pipe(
        addHistoricalBlockQuery$({
          ctx,
          forceCurrentBlockNumber,
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
          getCallParams: (item) => ({
            chain: chain,
            contractAddress:
              item.productData.type === "beefy:boost" ? item.productData.boost.contract_address : item.productData.vault.contract_address,
          }),
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        // add this block to our global block list
        upsertBlock$({
          ctx: {
            ...ctx,
            emitErrors: () => {
              throw new Error("Failed to upsert block");
            },
          },
          getBlockData: (item) => ({
            datetime: item.contractCreationInfo.datetime,
            chain: item.chain,
            blockNumber: item.contractCreationInfo.blockNumber,
            blockData: {},
          }),
          formatOutput: (item, block) => ({ ...item, block }),
        }),

        Rx.map((item) => ({
          type: "product:investment",
          productId: item.productId,
          chain: item.chain,
          chainLatestBlockNumber: item.contractCreationInfo.blockNumber,
          contractCreatedAtBlock: item.contractCreationInfo.blockNumber,
          contractCreationDate: item.contractCreationInfo.datetime,
          ranges: {
            lastImportDate: new Date(),
            coveredRanges: [],
            toRetry: [],
          },
        })),
      ),
    processImportQuery$: (ctx) => importProductBlockRange$({ ctx, mode: "historical" }),
  });
}

export function importChainRecentData$(client: DbClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  return createRecentImportPipeline<DbBeefyProduct, number>({
    client,
    chain,
    cacheKey: "beefy:product:investment:recent",
    logInfos: { msg: "Importing recent beefy investments", data: { chain } },
    getImportStateKey,
    isLiveItem: isBeefyProductLive,
    generateQueries$: (ctx, lastImported) =>
      addLatestBlockQuery$({
        ctx,
        forceCurrentBlockNumber,
        getLastImportedBlock: () => lastImported,
        formatOutput: (item, latest, range) => ({ ...item, range, latest }),
      }),
    processImportQuery$: (ctx) => importProductBlockRange$({ ctx, mode: "recent" }),
  });
}

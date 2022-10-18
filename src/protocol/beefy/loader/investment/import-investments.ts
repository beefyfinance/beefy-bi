import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchContractCreationInfos$ } from "../../../common/connector/contract-creation";
import { addHistoricalBlockQuery$, addLatestBlockQuery$ } from "../../../common/connector/import-queries";
import { DbProductInvestmentImportState } from "../../../common/loader/import-state";
import { DbBeefyProduct } from "../../../common/loader/product";
import { createHistoricalImportPipeline, createRecentImportPipeline } from "../../../common/utils/historical-recent-pipeline";
import { isBeefyBoost } from "../../utils/type-guard";
import { importProductBlockRange$ } from "./product-block-range";

export function importChainHistoricalData$(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  return createHistoricalImportPipeline<DbBeefyProduct, number, DbProductInvestmentImportState>({
    client,
    chain,
    logInfos: { msg: "Importing historical beefy investments", data: { chain } },
    getImportStateKey: (product) => `product:investment:${product.productId}`,
    isLiveItem: (target) => (isBeefyBoost(target) ? target.productData.boost.eol : target.productData.vault.eol),
    generateQueries$: (ctx) =>
      addHistoricalBlockQuery$({
        rpcConfig: ctx.rpcConfig,
        streamConfig: ctx.streamConfig,
        forceCurrentBlockNumber,
        getImport: (item) => item.importState,
        getFirstBlockNumber: (importState) => importState.importData.contractCreatedAtBlock,
        formatOutput: (_, latestBlockNumber, blockQueries) => blockQueries.map((range) => ({ range, latest: latestBlockNumber })),
      }),
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
    processImportQuery$: (ctx) => importProductBlockRange$({ ctx }),
  });
}

export function importChainRecentData$(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  return createRecentImportPipeline<DbBeefyProduct, number>({
    client,
    chain,
    cacheKey: "beefy:product:investment:recent",
    logInfos: { msg: "Importing historical beefy investments", data: { chain } },
    isLiveItem: (target) => (isBeefyBoost(target) ? target.productData.boost.eol : target.productData.vault.eol),
    generateQueries$: (ctx, lastImported) =>
      addLatestBlockQuery$({
        rpcConfig: ctx.rpcConfig,
        forceCurrentBlockNumber,
        streamConfig: ctx.streamConfig,
        getLastImportedBlock: () => lastImported,
        formatOutput: (item, latest, range) => [{ ...item, range, latest }],
      }),
    processImportQuery$: (ctx) => importProductBlockRange$({ ctx }),
  });
}

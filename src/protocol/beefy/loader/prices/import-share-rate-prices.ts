import { get, mean, sortBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { db_query } from "../../../../utils/db";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { isInRange, Range } from "../../../../utils/range";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { addCoveredBlockListQuery, latestBlockNumber$ } from "../../../common/connector/import-queries";
import { fetchPriceFeedContractCreationInfos } from "../../../common/loader/fetch-product-creation-infos";
import { DbProductShareRateImportState } from "../../../common/loader/import-state";
import { DbPriceFeed } from "../../../common/loader/price-feed";
import { upsertPrice$ } from "../../../common/loader/prices";
import { fetchProduct$ } from "../../../common/loader/product";
import { ImportCtx } from "../../../common/types/import-context";
import { ImportPointQuery, ImportPointResult } from "../../../common/types/import-query";
import { createHistoricalImportPipeline } from "../../../common/utils/historical-recent-pipeline";
import { fetchBeefyPPFS$ } from "../../connector/ppfs";
import { isBeefyBoost, isBeefyGovVault } from "../../utils/type-guard";

export function importBeefyHistoricalShareRatePrices$(options: { client: PoolClient; chain: Chain; forceCurrentBlockNumber: number | null }) {
  return createHistoricalImportPipeline<DbPriceFeed, number, DbProductShareRateImportState>({
    client: options.client,
    chain: options.chain, // unused
    logInfos: { msg: "Importing historical share rate prices", data: { chain: options.chain } },
    getImportStateKey: (priceFeed) => `price:feed:${priceFeed.priceFeedId}`,
    isLiveItem: (target) => target.priceFeedData.active,
    generateQueries$: (ctx) =>
      Rx.pipe(
        addCoveredBlockListQuery({
          ctx,
          chain: options.chain,
          forceCurrentBlockNumber: options.forceCurrentBlockNumber,
          getScopeImportState: (item) => item.importState,
          formatOutput: (item, latestBlockNumber, blockRanges) => ({ ...item, blockList }),
        }),
        Rx.pipe((item) => {}),
      ),
    createDefaultImportState$: (ctx) =>
      Rx.pipe(
        // initialize the import state

        // find the first date we are interested in this price
        // so we need the first creation date of each product
        fetchPriceFeedContractCreationInfos({
          ctx: {
            ...ctx,
            emitErrors: (item) => {
              throw new Error("Error while fetching product creation infos for price feed" + item.priceFeedId);
            },
          },
          which: "price-feed-1", // we work on the first applied price
          getPriceFeedId: (item) => item.priceFeedId,
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        Rx.map((item) => ({
          type: "product:share-rate",
          priceFeedId: item.priceFeedId,
          chain: item.contractCreationInfo.chain,
          productId: item.contractCreationInfo.productId,
          chainLatestBlockNumber: 0,
          contractCreatedAtBlock: item.contractCreationInfo.contractCreatedAtBlock,
          contractCreationDate: item.contractCreationInfo.contractCreationDate,
          ranges: {
            lastImportDate: new Date(),
            coveredRanges: [],
            toRetry: [],
          },
        })),
      ),
    processImportQuery$: (ctx) => processShareRateQuery$({ ctx }),
  });
}

function processShareRateQuery$<
  TObj extends ImportPointQuery<DbPriceFeed, number> & { importState: DbProductShareRateImportState },
  TCtx extends ImportCtx<TObj>,
>(options: { ctx: TCtx }): Rx.OperatorFunction<TObj, ImportPointResult<DbPriceFeed, number>> {
  return Rx.pipe(
    fetchProduct$({
      ctx: options.ctx,
      getProductId: (item) => item.importState.importData.productId,
      formatOutput: (item, product) => ({ ...item, product }),
    }),

    fetchBeefyPPFS$({
      ctx: options.ctx,
      getPPFSCallParams: (item) => {
        if (isBeefyBoost(item.product)) {
          throw new ProgrammerError("beefy boost do not have ppfs");
        }
        if (isBeefyGovVault(item.product)) {
          throw new ProgrammerError("beefy gov vaults do not have ppfs");
        }
        const vault = item.product.productData.vault;
        return {
          underlyingDecimals: vault.want_decimals,
          vaultAddress: vault.contract_address,
          vaultDecimals: vault.token_decimals,
          blockNumber: item.point,
        };
      },
      formatOutput: (item, ppfs) => ({ ...item, ppfs }),
    }),

    // add block datetime
    fetchBlockDatetime$({
      ctx: options.ctx,
      getBlockNumber: (item) => item.point,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),

    upsertPrice$({
      ctx: options.ctx,
      getPriceData: (item) => ({
        datetime: item.blockDatetime,
        blockNumber: item.point,
        priceFeedId: item.target.priceFeedId,
        price: item.ppfs,
        priceData: { from: "ppfs-snapshots" },
      }),
      formatOutput: (priceData, price) => ({ ...priceData, price }),
    }),

    // transform to result
    Rx.map((item) => ({ ...item, success: get(item, "success", false) })),
  );
}

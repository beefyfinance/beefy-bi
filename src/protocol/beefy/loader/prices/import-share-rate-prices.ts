import { get, sortBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { samplingPeriodMs } from "../../../../types/sampling";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { createObservableWithNext } from "../../../../utils/rxjs/utils/create-observable-with-next";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { addHistoricalBlockQuery$ } from "../../../common/connector/import-queries";
import {
  addMissingImportState$,
  DbProductShareRateImportState,
  isProductShareRateImportState,
  updateImportState$,
} from "../../../common/loader/import-state";
import { DbPriceFeed } from "../../../common/loader/price-feed";
import { upsertPrice$ } from "../../../common/loader/prices";
import { fetchProduct$ } from "../../../common/loader/product";
import { ImportQuery, ImportResult } from "../../../common/types/import-query";
import { BatchStreamConfig } from "../../../common/utils/batch-rpc-calls";
import { memoryBackpressure$ } from "../../../common/utils/memory-backpressure";
import { createRpcConfig } from "../../../common/utils/rpc-config";
import { fetchBeefyPPFS$ } from "../../connector/ppfs";
import { isBeefyBoost, isBeefyGovVault } from "../../utils/type-guard";
import { fetchProductContractCreationInfos } from "./fetch-product-creation-infos";

export function importBeefyHistoricalShareRatePrices$(options: { client: PoolClient; chain: Chain; forceCurrentBlockNumber: number | null }) {
  const logger = rootLogger.child({ module: "beefy", component: "import-historical-share-rate-prices" });
  const rpcConfig = createRpcConfig(options.chain);

  const streamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: 1,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 30 * 1000,
    maxInputTake: 500,
    // and we can affort longer retries
    maxTotalRetryMs: 30_000,
  };
  const {
    observable: priceFeedErrors$,
    complete: completePriceFeedErrors$,
    next: emitErrors,
  } = createObservableWithNext<ImportQuery<DbPriceFeed, number>>();

  const ctx = { client: options.client, rpcConfig, streamConfig, emitErrors };

  const getImportStateKey = (priceFeedId: number) => `price:feed:${priceFeedId}`;

  return Rx.pipe(
    Rx.pipe(
      Rx.tap((priceFeed: DbPriceFeed) => logger.debug({ msg: "fetching beefy ppfs prices", data: priceFeed })),

      // map to an object where we can add attributes to safely
      Rx.map((priceFeed) => ({ target: priceFeed })),

      // remove duplicates
      Rx.distinct(({ target }) => target.priceFeedData.externalId),
    ),

    addMissingImportState$({
      client: options.client,
      streamConfig,
      getImportStateKey: (item) => getImportStateKey(item.target.priceFeedId),
      createDefaultImportState$: Rx.pipe(
        // initialize the import state

        // find the first date we are interested in this price
        // so we need the first creation date of each product
        fetchProductContractCreationInfos({
          ctx: {
            ...ctx,
            emitErrors: (item) => {
              throw new Error("Error while fetching product creation infos for price feed" + item.target.priceFeedId);
            },
          },
          getPriceFeedId: (item) => item.target.priceFeedId,
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        Rx.map((item) => ({
          type: "product:share-rate",
          priceFeedId: item.target.priceFeedId,
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
      formatOutput: (item, importState) => ({ ...item, importState }),
    }),

    // fix ts types
    Rx.filter((item): item is { target: DbPriceFeed; importState: DbProductShareRateImportState } => isProductShareRateImportState(item.importState)),

    // process first the live prices we imported the least
    Rx.pipe(
      Rx.toArray(),
      Rx.map((items) =>
        sortBy(
          items,
          (item) =>
            item.importState.importData.ranges.lastImportDate.getTime() + (item.target.priceFeedData.active ? 0 : samplingPeriodMs["1day"] * 100),
        ),
      ),
      Rx.concatAll(),
    ),

    Rx.pipe(
      // make it an import query
      Rx.map((item) => ({ ...item, latest: 0, range: { from: 0, to: 0 } })),

      // add the product infos
      fetchProduct$({
        ctx,
        getProductId: (item) => item.importState.importData.productId,
        formatOutput: (item, product) => ({ ...item, product }),
      }),

      // generate the block ranges to import
      addHistoricalBlockQuery$({
        rpcConfig,
        forceCurrentBlockNumber: options.forceCurrentBlockNumber,
        streamConfig,
        getImport: (item) => item.importState as DbProductShareRateImportState,
        getFirstBlockNumber: (importState) => importState.importData.contractCreatedAtBlock,
        formatOutput: (item, latestBlockNumber, blockQueries) => ({ ...item, blockQueries, latest: latestBlockNumber }),
      }),

      // convert to stream of price queries
      Rx.concatMap((item) =>
        item.blockQueries.map((range) => {
          const { blockQueries, ...rest } = item;
          return { ...rest, range, latest: item.latest };
        }),
      ),

      // some backpressure mechanism
      Rx.pipe(
        memoryBackpressure$({
          logInfos: { msg: "import-historical-share-rate-price-data" },
          sendBurstsOf: streamConfig.maxInputTake,
        }),

        Rx.tap((item) =>
          logger.info({
            msg: "processing share rate query",
            data: { feedKey: item.target.feedKey, range: item.range },
          }),
        ),
      ),
    ),

    Rx.pipe(
      fetchBeefyPPFS$({
        ctx,
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
            blockNumber: item.range.from,
          };
        },
        formatOutput: (item, ppfs) => ({ ...item, ppfs }),
      }),

      // add block datetime
      fetchBlockDatetime$({
        ctx,
        getBlockNumber: (item) => item.range.from,
        formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
      }),

      upsertPrice$({
        ctx,
        getPriceData: (item) => ({
          datetime: item.blockDatetime,
          blockNumber: item.range.from,
          priceFeedId: item.target.priceFeedId,
          price: item.ppfs,
          priceData: {},
        }),
        formatOutput: (priceData, price) => ({ ...priceData, price }),
      }),
    ),

    Rx.pipe(
      // handle the results
      Rx.pipe(
        Rx.map((item) => ({ ...item, success: get(item, "success", true) })),
        // make sure we close the errors observable when we are done
        Rx.finalize(() => setTimeout(completePriceFeedErrors$)),
        // merge the errors back in, all items here should have been successfully treated
        Rx.mergeWith(priceFeedErrors$.pipe(Rx.map((item) => ({ ...item, success: false })))),
        // make sure the type is correct
        Rx.map((item): ImportResult<DbPriceFeed, number> => item),
      ),

      // update the import state
      updateImportState$({
        client: options.client,
        streamConfig,
        getRange: (item) => item.range,
        isSuccess: (item) => item.success,
        getImportStateKey: (item) => getImportStateKey(item.target.priceFeedId),
        formatOutput: (item) => item,
      }),
    ),
  );
}

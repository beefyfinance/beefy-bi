import * as Rx from "rxjs";
import { PoolClient } from "pg";
import { fetchBeefyPrices } from "../connector/prices";
import Decimal from "decimal.js";
import { DbPriceFeed } from "../../common/loader/price-feed";
import { findMissingPriceRangeInDb$, upsertPrices$ } from "../../common/loader/prices";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { rootLogger } from "../../../utils/logger";

const logger = rootLogger.child({ module: "beefy", component: "import-prices" });

export function importBeefyPrices$(options: { client: PoolClient }) {
  return Rx.pipe(
    Rx.pipe(
      Rx.tap((priceFeed: DbPriceFeed) => logger.debug({ msg: "fetching beefy prices", data: priceFeed })),

      // only live price feeds
      Rx.filter((priceFeed) => priceFeed.priceFeedData.is_active),

      // map to an object where we can add attributes to safely
      Rx.map((priceFeed) => ({ priceFeed })),

      // remove duplicates
      Rx.distinct(({ priceFeed }) => priceFeed.externalId),
    ),

    // find which data is missing
    findMissingPriceRangeInDb$({
      client: options.client,
      getFeedId: (productData) => productData.priceFeed.priceFeedId,
      formatOutput: (productData, missingData) => ({ ...productData, missingData }),
    }),

    Rx.pipe(
      // be nice with beefy api plz
      rateLimit$(300),

      // now we fetch
      Rx.mergeMap(async (productData) => {
        const debugLogData = {
          priceFeedKey: productData.priceFeed.feedKey,
          priceFeed: productData.priceFeed.priceFeedId,
          missingData: productData.missingData,
        };

        logger.debug({ msg: "fetching prices", data: debugLogData });
        const prices = await fetchBeefyPrices("15min", productData.priceFeed.externalId, {
          startDate: productData.missingData.fromDate,
          endDate: productData.missingData.toDate,
        });
        logger.debug({ msg: "got prices", data: { ...debugLogData, priceCount: prices.length } });

        return { ...productData, prices };
      }, 1 /* concurrency */),

      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.concatMap((productData) => Rx.from(productData.prices.map((price) => ({ ...productData, price })))),
    ),

    upsertPrices$({
      client: options.client,
      getPriceData: (priceData) => ({
        datetime: priceData.price.datetime,
        priceFeedId: priceData.priceFeed.priceFeedId,
        usdValue: new Decimal(priceData.price.value),
      }),
      formatOutput: (priceData, price) => ({ ...priceData, price }),
    }),
  );
}

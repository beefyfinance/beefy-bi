import * as Rx from "rxjs";
import yargs from "yargs";
import { allChainIds, Chain } from "../../../types/chain";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { DbClient, db_query, withPgClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { runMain } from "../../../utils/process";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { upsertPrice$ } from "../../common/loader/prices";
import { DbBeefyStdVaultProduct, fetchProduct$ } from "../../common/loader/product";
import { createRpcConfig } from "../../common/utils/rpc-config";
import { fetchBeefyPPFS$ } from "../connector/ppfs";

const logger = rootLogger.child({ module: "script", component: "fix-ppfs-anomalies" });

async function main(client: DbClient) {
  // add a migrate command
  const argv = await yargs.options({
    chain: {
      type: "array",
      choices: [...allChainIds, "all"],
      alias: "c",
      demand: false,
      default: "all",
      describe: "only import data for this chain",
    },
    thresholdPercent: { type: "number", demand: false, default: 0.1, describe: "threshold for anomaly detection" },
    windowHalfSize: { type: "number", demand: false, default: 10, describe: "window size for anomaly detection" },
    contractAddress: { type: "string", demand: false, alias: "a", describe: "only import data for this contract address" },
    currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "Force the current block number" },
  }).argv;

  const chain = argv.chain === "all" ? allChainIds : ([argv.chain] as Chain[]);
  const thresholdPercent = argv.thresholdPercent;
  const windowHalfSize = argv.windowHalfSize;
  const contractAddress = argv.contractAddress || null;

  return Promise.all(
    chain.map(async (chain) => {
      const pipeline$ = fixPPfsAnomalies$({ chain, client, contractAddress, thresholdPercent, windowHalfSize });
      return consumeObservable(pipeline$);
    }),
  );
}

function fixPPfsAnomalies$({
  chain,
  client,
  thresholdPercent,
  windowHalfSize,
  contractAddress,
}: {
  chain: Chain;
  client: DbClient;
  thresholdPercent: number;
  windowHalfSize: number;
  contractAddress: string | null;
}) {
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
  const emitError = (item: any) => logger.error({ msg: "error fixing anomaly", data: { item } });

  type PPFSAnomaly = {
    product_id: number;
    price_feed_id: number;
    datetime: string;
    block_number: number;
    price: string;
    avg_price: string;
    dst: string;
    threshold: string;
    is_anomaly: boolean;
  };
  return Rx.of(
    db_query<PPFSAnomaly>(
      `
        select *
        from (
            select price_feed_id, datetime, block_number, price, 
                avg(price) OVER w avg_price, 
                abs(avg(price) OVER w - price) as dst,
                %L * avg(price) OVER w as threshold,
                (abs(avg(price) OVER w - price)) > (%L * avg(price) OVER w) as is_anomaly,
                price_data
            from price_ts 
            join product p on p.price_feed_1_id = price_ts.price_feed_id
            where p.chain in (%L)
            window w as (partition by price_feed_id ORDER BY block_number rows BETWEEN %L PRECEDING AND %L FOLLOWING)
        ) as t
        where is_anomaly = true
      `,
      [thresholdPercent, thresholdPercent, chain, windowHalfSize, windowHalfSize],
      client,
    ),
  ).pipe(
    Rx.pipe(
      Rx.mergeAll(),
      Rx.tap((anomaliesToRetry) => logger.debug({ msg: "found anomalies", data: { count: anomaliesToRetry.length } })),
      Rx.concatAll(),
      Rx.map((anomaly) => ({ anomaly })),
    ),

    // we want the decimals for the product linked to the price feed
    fetchProduct$({ ctx, emitError, getProductId: (item) => item.anomaly.product_id, formatOutput: (item, product) => ({ ...item, product }) }),

    // only care about vaults
    Rx.filter((item): item is { anomaly: PPFSAnomaly; product: DbBeefyStdVaultProduct } => item.product.productData.type === "beefy:vault"),
    // apply contract address filter
    Rx.filter((item) => !contractAddress || item.product.productData.vault.contract_address === contractAddress),

    // for each anomaly, re-fetch the ppfs data
    fetchBeefyPPFS$({
      ctx,
      emitError,
      getPPFSCallParams: (item) => ({
        blockNumber: item.anomaly.block_number,
        underlyingDecimals: item.product.productData.vault.want_decimals,
        vaultAddress: item.product.productData.vault.contract_address,
        vaultDecimals: item.product.productData.vault.token_decimals,
      }),
      formatOutput: (item, ppfs) => ({ ...item, ppfs }),
    }),

    // update with the latest value
    upsertPrice$({
      ctx,
      emitError,
      getPriceData: (item) => ({
        datetime: new Date(item.anomaly.datetime),
        blockNumber: item.anomaly.block_number,
        priceFeedId: item.anomaly.price_feed_id,
        price: item.ppfs,
        // json data is merged with the existing data
        priceData: { fixedByScript: { script: "fix-ppfs-anomalies", when: new Date().toISOString() } },
      }),
      formatOutput: (priceData, price) => ({ ...priceData, price }),
    }),
  );
}

runMain(withPgClient(main, { appName: "beefy:fix_ppfs_anomalies_script", logInfos: { msg: "fix_ppfs_anomalies_script" } }));

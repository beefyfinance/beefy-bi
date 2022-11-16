import * as Rx from "rxjs";
import yargs from "yargs";
import { allChainIds, Chain } from "../../../types/chain";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { DbClient, db_query, withPgClient } from "../../../utils/db";
import { normalizeAddress } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { runMain } from "../../../utils/process";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { upsertPrice$ } from "../../common/loader/prices";
import { productList$ } from "../../common/loader/product";
import { createRpcConfig } from "../../common/utils/rpc-config";
import { fetchBeefyPPFS$ } from "../connector/ppfs";
import { isBeefyStandardVault } from "../utils/type-guard";

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
    thresholdPercent: { type: "number", demand: false, default: 0.1, alias: "t", describe: "threshold for anomaly detection" },
    avgWindowHalfSize: { type: "number", demand: false, default: 10, alias: "v", describe: "window size for anomaly detection using averages" },
    spikeWindowHalfSize: { type: "number", demand: false, default: 1, alias: "s", describe: "window size for anomaly detection using spikes" },
    contractAddress: { type: "string", demand: false, alias: "a", describe: "only import data for this contract address" },
    currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "Force the current block number" },
  }).argv;

  const chain = argv.chain.includes("all") ? allChainIds : (argv.chain as unknown as Chain[]);
  const thresholdPercent = argv.thresholdPercent;
  const avgWindowHalfSize = argv.avgWindowHalfSize;
  const spikeWindowHalfSize = argv.spikeWindowHalfSize;
  const contractAddress = argv.contractAddress || null;

  return Promise.all(
    chain.map(async (chain) => {
      const pipeline$ = fixPPfsAnomalies$({ chain, client, contractAddress, thresholdPercent, avgWindowHalfSize, spikeWindowHalfSize });
      return consumeObservable(pipeline$);
    }),
  );
}

function fixPPfsAnomalies$({
  chain,
  client,
  thresholdPercent,
  avgWindowHalfSize,
  spikeWindowHalfSize,
  contractAddress,
}: {
  chain: Chain;
  client: DbClient;
  thresholdPercent: number;
  avgWindowHalfSize: number;
  spikeWindowHalfSize: number;
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
    first_spike_value: string;
    last_spike_value: string;
    is_below_anomaly: boolean;
    is_above_anomaly: boolean;
  };
  return Rx.of(productList$(client, "beefy:vault", chain)).pipe(
    Rx.concatAll(),

    // only care about vaults
    Rx.filter(isBeefyStandardVault),

    Rx.map((product) => ({ product })),

    // apply contract address filter
    Rx.filter(
      (item) => contractAddress === null || normalizeAddress(item.product.productData.vault.contract_address) === normalizeAddress(contractAddress),
    ),

    Rx.concatMap(async (item) => {
      const res = await db_query<PPFSAnomaly>(
        `
        select *
        from (
            select p.product_id, 
                price_feed_id, 
                datetime,
                block_number,
                price, 
                avg(price) OVER wAvg avg_price, 
                abs(avg(price) OVER wAvg - price) as dst,
                %L * avg(price) OVER wAvg as threshold,
                (abs(avg(price) OVER wAvg - price)) > (*L * avg(price) OVER wAvg) as is_anomaly,
                first_value(price) over wSpike as first_spike_value,
                last_value(price) over wSpike as last_spike_value,
                price > first_value(price) over wSpike and price > last_value(price) over wSpike as is_above_anomaly,
                price < first_value(price) over wSpike and price < last_value(price) over wSpike as is_below_anomaly,
                price_data
            from price_ts 
            join product p on p.price_feed_1_id = price_ts.price_feed_id
            where price_feed_id = %L
            window wAvg as (partition by price_feed_id ORDER BY block_number rows BETWEEN %L PRECEDING AND %L FOLLOWING),
            wSpike as (partition by price_feed_id ORDER BY block_number rows BETWEEN %L PRECEDING AND %L FOLLOWING)
        ) as t
        where is_anomaly = true or is_above_anomaly = true or is_below_anomaly = true
      `,
        [
          thresholdPercent,
          thresholdPercent,
          item.product.priceFeedId1,
          avgWindowHalfSize,
          avgWindowHalfSize,
          spikeWindowHalfSize,
          spikeWindowHalfSize,
        ],
        client,
      );
      return res.map((anomaly) => ({ ...item, anomaly }));
    }),
    Rx.pipe(
      Rx.tap((anomaliesToRetry) => logger.debug({ msg: "found anomalies", data: { count: anomaliesToRetry.length } })),
      Rx.concatAll(),
    ),

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

import { PoolClient } from "pg";
import * as Rx from "rxjs";
import yargs from "yargs";
import { allChainIds, Chain } from "../../../types/chain";
import { samplingPeriodMs } from "../../../types/sampling";
import { sleep } from "../../../utils/async";
import { db_migrate, withPgClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { fetchPriceFeed$, priceFeedList$ } from "../../common/loader/price-feed";
import { DbBeefyProduct, DbProduct, productList$ } from "../../common/loader/product";
import { defaultHistoricalStreamConfig } from "../../common/utils/historical-recent-pipeline";
import { createRpcConfig } from "../../common/utils/rpc-config";
import { importChainHistoricalData$, importChainRecentData$ } from "../loader/investment/import-investments";
import { importBeefyHistoricalShareRatePrices$ } from "../loader/prices/import-share-rate-prices";
import { importBeefyHistoricalUnderlyingPrices$, importBeefyRecentUnderlyingPrices$ } from "../loader/prices/import-underlying-prices";
import { importBeefyProducts$ } from "../loader/products";
import { isBeefyBoost, isBeefyStandardVault } from "../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "import-script" });

export function addBeefyCommands<TOptsBefore>(yargs: yargs.Argv<TOptsBefore>) {
  return yargs
    .command({
      command: "beefy:daemon:historical",
      describe: "Start the beefy importer daemon for historical data",
      builder: (yargs) =>
        yargs.options({
          chain: { choices: [...allChainIds, "all"], alias: "c", demand: false, default: "all", describe: "only import data for this chain" },
        }),
      handler: async (argv) =>
        withPgClient(async (client) => {
          await db_migrate();

          const chain = argv.chain as Chain | "all";
          const filterChains = chain === "all" ? allChainIds : [chain];

          return new Promise<any>(async () => {
            for (const chain of filterChains) {
              daemonize(
                `investment-historical-${chain}`,
                () => importInvestmentData({ client, strategy: "historical", chain, filterContractAddress: null, forceCurrentBlockNumber: null }),
                samplingPeriodMs["15s"],
              );
            }

            daemonize("historical-prices", () => importBeefyDataPrices({ client, which: "historical" }), samplingPeriodMs["5min"]);

            for (const chain of filterChains) {
              daemonize(
                `share-rate-historical-${chain}`,
                () => importBeefyDataShareRate({ client, chain, filterContractAddress: null, forceCurrentBlockNumber: null }),
                samplingPeriodMs["15min"],
              );
            }
          });
        })(),
    })
    .command({
      command: "beefy:daemon:recent",
      describe: "Start the beefy importer daemon for recent data",
      builder: (yargs) =>
        yargs.options({
          chain: { choices: [...allChainIds, "all"], alias: "c", demand: false, default: "all", describe: "only import data for this chain" },
        }),
      handler: async (argv) =>
        withPgClient(async (client) => {
          await db_migrate();

          const chain = argv.chain as Chain | "all";
          const filterChains = chain === "all" ? allChainIds : [chain];

          return new Promise<any>(async () => {
            for (const chain of filterChains) {
              daemonize(
                `investment-recent-${chain}`,
                () => importInvestmentData({ client, strategy: "recent", chain, filterContractAddress: null, forceCurrentBlockNumber: null }),
                samplingPeriodMs["15min"],
              );
            }

            daemonize("recent-prices", () => importBeefyDataPrices({ client, which: "recent" }), samplingPeriodMs["15min"]);
          });
        })(),
    })
    .command({
      command: "beefy:daemon:products",
      describe: "Import products at regular intervals",
      builder: (yargs) =>
        yargs.options({
          chain: { choices: [...allChainIds, "all"], alias: "c", demand: false, default: "all", describe: "only import data for this chain" },
        }),
      handler: async (argv) =>
        withPgClient(async (client) => {
          await db_migrate();

          const chain = argv.chain as Chain | "all";
          const filterChains = chain === "all" ? allChainIds : [chain];

          return new Promise<any>(async () => {
            daemonize("products", () => importProducts({ client, filterChains }), samplingPeriodMs["1day"]);
          });
        })(),
    })
    .command({
      command: "beefy:run",
      describe: "Start a single beefy import",
      builder: (yargs) =>
        yargs.options({
          chain: { choices: [...allChainIds, "all"], alias: "c", demand: false, default: "all", describe: "only import data for this chain" },
          contractAddress: { type: "string", demand: false, alias: "a", describe: "only import data for this contract address" },
          currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "Force the current block number" },
          importer: {
            choices: ["historical", "recent", "products", "recent-prices", "historical-prices", "historical-share-rate"],
            demand: true,
            alias: "i",
            describe: "what to run, all if not specified",
          },
        }),
      handler: (argv): Promise<any> =>
        withPgClient((client) => {
          logger.info("Starting import script", { argv });

          const chain = argv.chain as Chain | "all";
          const filterContractAddress = argv.contractAddress || null;
          const forceCurrentBlockNumber = argv.currentBlockNumber || null;

          if (forceCurrentBlockNumber !== null && chain === "all") {
            throw new ProgrammerError("Cannot force current block number without a chain filter");
          }

          logger.trace({ msg: "starting", data: { chain, filterContractAddress } });

          switch (argv.importer) {
            case "historical":
              if (chain === "all") {
                return Promise.all(
                  allChainIds.map((chain) =>
                    importInvestmentData({ client, forceCurrentBlockNumber, strategy: "historical", chain, filterContractAddress }),
                  ),
                );
              } else {
                return importInvestmentData({ client, forceCurrentBlockNumber, strategy: "historical", chain, filterContractAddress });
              }
            case "recent":
              if (chain === "all") {
                return Promise.all(
                  allChainIds.map((chain) =>
                    importInvestmentData({ client, forceCurrentBlockNumber, strategy: "recent", chain, filterContractAddress }),
                  ),
                );
              } else {
                return importInvestmentData({ client, forceCurrentBlockNumber, strategy: "recent", chain, filterContractAddress });
              }
            case "products":
              return importProducts({ client, filterChains: chain === "all" ? allChainIds : [chain] });
            case "recent-prices":
              return importBeefyDataPrices({ client, which: "recent" });
            case "historical-prices":
              return importBeefyDataPrices({ client, which: "historical" });
            case "historical-share-rate":
              if (chain === "all") {
                return Promise.all(
                  allChainIds.map((chain) => importBeefyDataShareRate({ client, forceCurrentBlockNumber, chain, filterContractAddress })),
                );
              } else {
                return importBeefyDataShareRate({ client, forceCurrentBlockNumber, chain, filterContractAddress });
              }
            default:
              throw new ProgrammerError(`Unknown importer: ${argv.importer}`);
          }
        })(),
    });
}

async function daemonize<TRes>(name: string, fn: () => Promise<TRes>, sleepMs: number) {
  while (true) {
    try {
      logger.info({ msg: "starting daemon task", data: { name } });
      await fn();
      logger.info({ msg: "daemon task ended", data: { name } });
    } catch (e) {
      logger.error({ msg: "error in daemon task", data: { name, e } });
    }
    await sleep(sleepMs);
  }
}

async function importProducts(options: { client: PoolClient; filterChains: Chain[] }) {
  const pipeline$ = Rx.from(options.filterChains).pipe(importBeefyProducts$({ client: options.client }));
  logger.info({ msg: "starting product list import", data: { ...options, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

async function importBeefyDataPrices(options: { client: PoolClient; which: "recent" | "historical" }) {
  const pipeline$ = priceFeedList$(options.client, "beefy-data").pipe(
    options.which === "recent"
      ? importBeefyRecentUnderlyingPrices$({ client: options.client })
      : importBeefyHistoricalUnderlyingPrices$({ client: options.client }),
  );
  logger.info({ msg: "starting prices import", data: { ...options, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

const investmentPipelineByChain = {} as Record<
  Chain,
  { historical: Rx.OperatorFunction<DbBeefyProduct, any>; recent: Rx.OperatorFunction<DbBeefyProduct, any> }
>;
function getChainInvestmentPipeline(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  if (!investmentPipelineByChain[chain]) {
    investmentPipelineByChain[chain] = {
      historical: importChainHistoricalData$(client, chain, forceCurrentBlockNumber),
      recent: importChainRecentData$(client, chain, forceCurrentBlockNumber),
    };
  }
  return investmentPipelineByChain[chain];
}

async function importInvestmentData(options: {
  client: PoolClient;
  strategy: "recent" | "historical";
  chain: Chain;
  filterContractAddress: string | null;
  forceCurrentBlockNumber: number | null;
}) {
  const { recent, historical } = getChainInvestmentPipeline(options.client, options.chain, options.forceCurrentBlockNumber);
  const pipeline$ = productList$(options.client, "beefy", options.chain).pipe(
    Rx.filter((product) => product.chain === options.chain),
    Rx.filter((product) => {
      const contractAddress =
        product.productData.type === "beefy:boost" ? product.productData.boost.contract_address : product.productData.vault.contract_address;
      return options.filterContractAddress === null || contractAddress.toLocaleLowerCase() === options.filterContractAddress.toLocaleLowerCase();
    }),
    options.strategy === "recent" ? recent : historical,
  );
  logger.info({ msg: "starting investment data import", data: { ...options, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

function importBeefyDataShareRate(options: {
  client: PoolClient;
  chain: Chain;
  filterContractAddress: string | null;
  forceCurrentBlockNumber: number | null;
}) {
  const productFilter$ = Rx.pipe(
    Rx.filter((product: DbProduct) => product.chain === options.chain),
    Rx.filter((product) => {
      const contractAddress =
        product.productData.type === "beefy:boost" ? product.productData.boost.contract_address : product.productData.vault.contract_address;
      return options.filterContractAddress === null || contractAddress.toLocaleLowerCase() === options.filterContractAddress.toLocaleLowerCase();
    }),
  );

  const rpcConfig = createRpcConfig(options.chain);
  const streamConfig = defaultHistoricalStreamConfig;
  const ctx = {
    client: options.client,
    emitErrors: (item: DbProduct) => {
      throw new Error(`Error fetching price feed for product ${item.productId}`);
    },
    rpcConfig,
    streamConfig,
  };

  const pipeline$ = productList$(options.client, "beefy", options.chain).pipe(
    productFilter$,
    // remove products that don't have a ppfs to fetch
    Rx.filter((product) => isBeefyStandardVault(product) || isBeefyBoost(product)),
    // now fetch the price feed we need
    fetchPriceFeed$({ ctx, getPriceFeedId: (product) => product.priceFeedId1, formatOutput: (_, priceFeed) => ({ priceFeed }) }),
    // drop those without a price feed yet
    excludeNullFields$("priceFeed"),
    Rx.map(({ priceFeed }) => priceFeed),
    // remove duplicates
    Rx.distinct((priceFeed) => priceFeed.priceFeedId),
    // now fetch associated price feeds
    importBeefyHistoricalShareRatePrices$({ client: options.client, chain: options.chain, forceCurrentBlockNumber: options.forceCurrentBlockNumber }),
  );

  logger.info({ msg: "starting share rate data import", data: { ...options, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

import * as Rx from "rxjs";
import yargs from "yargs";
import { allChainIds, Chain } from "../../../types/chain";
import { allSamplingPeriods, SamplingPeriod, samplingPeriodMs } from "../../../types/sampling";
import { sleep } from "../../../utils/async";
import { DbClient, withPgClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { fetchPriceFeed$ } from "../../common/loader/price-feed";
import { DbBeefyProduct, DbProduct, productList$ } from "../../common/loader/product";
import { defaultHistoricalStreamConfig } from "../../common/utils/historical-recent-pipeline";
import { createRpcConfig } from "../../common/utils/rpc-config";
import { importChainHistoricalData$, importChainRecentData$ } from "../loader/investment/import-investments";
import { importBeefyHistoricalShareRatePrices$ } from "../loader/prices/import-share-rate-prices";
import { importBeefyHistoricalUnderlyingPrices$, importBeefyRecentUnderlyingPrices$ } from "../loader/prices/import-underlying-prices";
import { importBeefyProducts$ } from "../loader/products";
import { isBeefyBoost, isBeefyProductLive, isBeefyStandardVault } from "../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "import-script" });

interface CmdParams {
  client: DbClient;
  task: "historical" | "recent" | "products" | "recent-prices" | "historical-prices" | "historical-share-rate";
  filterChains: Chain[];
  includeEol: boolean;
  forceCurrentBlockNumber: number | null;
  filterContractAddress: string | null;
  repeatEvery: SamplingPeriod | null;
}

export function addBeefyCommands<TOptsBefore>(yargs: yargs.Argv<TOptsBefore>) {
  return yargs.command({
    command: "beefy:run",
    describe: "Start a single beefy import",
    builder: (yargs) =>
      yargs.options({
        chain: {
          type: "array",
          choices: [...allChainIds, "all"],
          alias: "c",
          demand: false,
          default: "all",
          describe: "only import data for this chain",
        },
        contractAddress: { type: "string", demand: false, alias: "a", describe: "only import data for this contract address" },
        currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "Force the current block number" },
        includeEol: { type: "boolean", demand: false, default: false, alias: "e", describe: "Include EOL products for some chain" },
        task: {
          choices: ["historical", "recent", "products", "recent-prices", "historical-prices", "historical-share-rate"],
          demand: true,
          alias: "t",
          describe: "what to run",
        },
        repeatEvery: { choices: allSamplingPeriods, demand: false, alias: "r", describe: "repeat the task from time to time" },
      }),
    handler: (argv): Promise<any> =>
      withPgClient(
        async (client) => {
          //await db_migrate();

          logger.info("Starting import script", { argv });

          const cmdParams: CmdParams = {
            client,
            task: argv.task as CmdParams["task"],
            includeEol: argv.includeEol,
            filterChains: argv.chain.includes("all") ? allChainIds : (argv.chain as Chain[]),
            filterContractAddress: argv.contractAddress || null,
            forceCurrentBlockNumber: argv.currentBlockNumber || null,
            repeatEvery: argv.repeatEvery || null,
          };
          if (cmdParams.forceCurrentBlockNumber !== null && cmdParams.filterChains.length > 1) {
            throw new ProgrammerError("Cannot force current block number without a chain filter");
          }

          const tasks = getTasksToRun(cmdParams);

          return Promise.all(
            tasks.map(async (task) => {
              do {
                const start = Date.now();
                await task();
                const now = Date.now();

                logger.info({ msg: "Import task finished" });

                if (cmdParams.repeatEvery !== null) {
                  const shouldSleepABit = now - start < samplingPeriodMs[cmdParams.repeatEvery];
                  if (shouldSleepABit) {
                    const sleepTime = samplingPeriodMs[cmdParams.repeatEvery] - (now - start);
                    logger.info({ msg: "Sleeping after import", data: { sleepTime } });
                    await sleep(sleepTime);
                  }
                }
              } while (cmdParams.repeatEvery !== null);
            }),
          );
        },
        { appName: "beefy:run", readOnly: false, logInfos: { msg: "beefy script", data: { task: argv.task, chains: argv.chain } } },
      )(),
  });
}

function getTasksToRun(cmdParams: CmdParams) {
  logger.trace({ msg: "starting", data: { ...cmdParams, client: "<redacted>" } });

  switch (cmdParams.task) {
    case "historical":
      return cmdParams.filterChains.map((chain) => () => importInvestmentData(chain, cmdParams));
    case "recent":
      return cmdParams.filterChains.map((chain) => () => importInvestmentData(chain, cmdParams));
    case "products":
      return [() => importProducts(cmdParams)];
    case "recent-prices":
      return [() => importBeefyDataPrices(cmdParams)];
    case "historical-prices":
      return [() => importBeefyDataPrices(cmdParams)];
    case "historical-share-rate":
      return cmdParams.filterChains.map((chain) => () => importBeefyDataShareRate(chain, cmdParams));
    default:
      throw new ProgrammerError(`Unknown importer: ${cmdParams.task}`);
  }
}

async function importProducts(cmdParams: CmdParams) {
  const pipeline$ = Rx.from(cmdParams.filterChains).pipe(importBeefyProducts$({ client: cmdParams.client }));
  logger.info({ msg: "starting product list import", data: { ...cmdParams, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

function importBeefyDataPrices(cmdParams: CmdParams) {
  const rpcConfig = createRpcConfig("bsc"); // never used
  const streamConfig = defaultHistoricalStreamConfig;
  const ctx = {
    chain: "bsc" as Chain, // not used here
    client: cmdParams.client,
    rpcConfig,
    streamConfig,
  };
  const emitError = (item: DbProduct) => {
    throw new Error(`Error fetching price feed for product ${item.productId}`);
  };

  const pipeline$ = productList$(cmdParams.client, "beefy", null).pipe(
    productFilter$(null, cmdParams),
    // remove products that don't have a ppfs to fetch
    Rx.filter((product) => isBeefyStandardVault(product) || isBeefyBoost(product)),
    // now fetch the price feed we need
    fetchPriceFeed$({ ctx, emitError, getPriceFeedId: (product) => product.priceFeedId2, formatOutput: (_, priceFeed) => ({ priceFeed }) }),
    // drop those without a price feed yet
    excludeNullFields$("priceFeed"),
    Rx.map(({ priceFeed }) => priceFeed),
    // remove duplicates
    Rx.distinct((priceFeed) => priceFeed.priceFeedId),
    // now import data for those
    cmdParams.task === "recent-prices"
      ? importBeefyRecentUnderlyingPrices$({ client: cmdParams.client })
      : importBeefyHistoricalUnderlyingPrices$({ client: cmdParams.client }),
  );

  logger.info({ msg: "starting share rate data import", data: { ...cmdParams, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

const investmentPipelineByChain = {} as Record<
  Chain,
  { historical: Rx.OperatorFunction<DbBeefyProduct, any>; recent: Rx.OperatorFunction<DbBeefyProduct, any> }
>;
function getChainInvestmentPipeline(chain: Chain, cmdParams: CmdParams) {
  if (!investmentPipelineByChain[chain]) {
    investmentPipelineByChain[chain] = {
      historical: importChainHistoricalData$(cmdParams.client, chain, cmdParams.forceCurrentBlockNumber),
      recent: importChainRecentData$(cmdParams.client, chain, cmdParams.forceCurrentBlockNumber),
    };
  }
  return investmentPipelineByChain[chain];
}

async function importInvestmentData(chain: Chain, cmdParams: CmdParams) {
  const { recent, historical } = getChainInvestmentPipeline(chain, cmdParams);
  const pipeline$ = productList$(cmdParams.client, "beefy", chain).pipe(
    productFilter$(chain, cmdParams),
    cmdParams.task === "recent" ? recent : historical,
  );
  logger.info({ msg: "starting investment data import", data: { ...cmdParams, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

function importBeefyDataShareRate(chain: Chain, cmdParams: CmdParams) {
  const rpcConfig = createRpcConfig(chain);
  const streamConfig = defaultHistoricalStreamConfig;
  const ctx = {
    chain,
    client: cmdParams.client,
    rpcConfig,
    streamConfig,
  };

  const emitError = (item: DbProduct) => {
    throw new Error(`Error fetching price feed for product ${item.productId}`);
  };

  const pipeline$ = productList$(cmdParams.client, "beefy", chain).pipe(
    productFilter$(chain, cmdParams),
    // remove products that don't have a ppfs to fetch
    // we don't fetch boosts because they would be duplicates anyway
    Rx.filter((product) => isBeefyStandardVault(product)),
    // now fetch the price feed we need
    fetchPriceFeed$({ ctx, emitError, getPriceFeedId: (product) => product.priceFeedId1, formatOutput: (_, priceFeed) => ({ priceFeed }) }),
    // drop those without a price feed yet
    excludeNullFields$("priceFeed"),
    Rx.map(({ priceFeed }) => priceFeed),
    // remove duplicates
    Rx.distinct((priceFeed) => priceFeed.priceFeedId),
    // now fetch associated price feeds
    importBeefyHistoricalShareRatePrices$({ chain: chain, ...cmdParams }),
  );

  logger.info({ msg: "starting share rate data import", data: { ...cmdParams, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

function productFilter$(chain: Chain | null, cmdParams: CmdParams) {
  return Rx.pipe(
    Rx.filter((product: DbProduct) => {
      if (chain === null) {
        return true;
      }
      return product.chain === chain;
    }),
    Rx.filter((product: DbProduct) => {
      const isLiveProduct = isBeefyProductLive(product);
      if (isLiveProduct) {
        return true;
      }

      return cmdParams.includeEol;
    }),
    Rx.toArray(),
    Rx.tap((items) => logger.info({ msg: "Import filtered by product", data: { count: items.length, chain, includeEol: cmdParams.includeEol } })),
    Rx.concatAll(),
    Rx.filter((product) => {
      const contractAddress =
        product.productData.type === "beefy:boost" ? product.productData.boost.contract_address : product.productData.vault.contract_address;
      return cmdParams.filterContractAddress === null || contractAddress.toLocaleLowerCase() === cmdParams.filterContractAddress.toLocaleLowerCase();
    }),
  );
}

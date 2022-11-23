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
import { fetchAllInvestorIds$ } from "../../common/loader/investment";
import { fetchInvestor$ } from "../../common/loader/investor";
import { DbPriceFeed, fetchPriceFeed$ } from "../../common/loader/price-feed";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbProduct, productList$ } from "../../common/loader/product";
import { defaultHistoricalStreamConfig } from "../../common/utils/multiplex-by-rpc";
import { createRpcConfig } from "../../common/utils/rpc-config";
import { importChainHistoricalData$, importChainRecentData$ } from "../loader/investment/import-investments";
import { importBeefyHistoricalPendingRewardsSnapshots$ } from "../loader/investment/import-pending-rewards-snapshots";
import { importBeefyHistoricalShareRatePrices$ } from "../loader/prices/import-share-rate-prices";
import { importBeefyHistoricalUnderlyingPrices$, importBeefyRecentUnderlyingPrices$ } from "../loader/prices/import-underlying-prices";
import { importBeefyProducts$ } from "../loader/products";
import { getProductContractAddress } from "../utils/contract-accessors";
import { isBeefyBoost, isBeefyGovVault, isBeefyProductLive, isBeefyStandardVault } from "../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "import-script" });

interface CmdParams {
  client: DbClient;
  rpcCount: number;
  forceRpcUrl: string | null;
  task: "historical" | "recent" | "products" | "recent-prices" | "historical-prices" | "historical-share-rate" | "reward-snapshots";
  filterChains: Chain[];
  includeEol: boolean;
  forceCurrentBlockNumber: number | null;
  filterContractAddress: string | null;
  loopEvery: SamplingPeriod | null;
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
        forceRpcUrl: { type: "string", demand: false, alias: "f", describe: "force a specific RPC URL" },
        includeEol: { type: "boolean", demand: false, default: false, alias: "e", describe: "Include EOL products for some chain" },
        task: {
          choices: ["historical", "recent", "products", "recent-prices", "historical-prices", "historical-share-rate", "reward-snapshots"],
          demand: true,
          alias: "t",
          describe: "what to run",
        },
        rpcCount: { type: "number", demand: false, default: 1, alias: "r", describe: "how many RPCs to use" },
        loopEvery: { choices: allSamplingPeriods, demand: false, alias: "l", describe: "repeat the task from time to time" },
      }),
    handler: (argv): Promise<any> =>
      withPgClient(
        async (client) => {
          //await db_migrate();

          logger.info("Starting import script", { argv });

          const cmdParams: CmdParams = {
            client,
            rpcCount: argv.rpcCount,
            task: argv.task as CmdParams["task"],
            includeEol: argv.includeEol,
            filterChains: argv.chain.includes("all") ? allChainIds : (argv.chain as Chain[]),
            filterContractAddress: argv.contractAddress || null,
            forceCurrentBlockNumber: argv.currentBlockNumber || null,
            forceRpcUrl: argv.forceRpcUrl || null,
            loopEvery: argv.loopEvery || null,
          };
          if (cmdParams.forceCurrentBlockNumber !== null && cmdParams.filterChains.length > 1) {
            throw new ProgrammerError("Cannot force current block number without a chain filter");
          }
          if (cmdParams.forceRpcUrl !== null && cmdParams.filterChains.length > 1) {
            throw new ProgrammerError("Cannot force RPC URL without a chain filter");
          }
          if (cmdParams.forceRpcUrl !== null && cmdParams.rpcCount !== 1) {
            throw new ProgrammerError("Cannot force RPC URL with multiple RPCs");
          }
          if (cmdParams.filterContractAddress !== null && cmdParams.filterChains.length > 1) {
            throw new ProgrammerError("Cannot filter contract address without a chain filter");
          }

          const tasks = getTasksToRun(cmdParams);

          return Promise.all(
            tasks.map(async (task) => {
              do {
                const start = Date.now();
                await task();
                const now = Date.now();

                logger.info({ msg: "Import task finished" });

                if (cmdParams.loopEvery !== null) {
                  const shouldSleepABit = now - start < samplingPeriodMs[cmdParams.loopEvery];
                  if (shouldSleepABit) {
                    const sleepTime = samplingPeriodMs[cmdParams.loopEvery] - (now - start);
                    logger.info({ msg: "Sleeping after import", data: { sleepTime } });
                    await sleep(sleepTime);
                  }
                }
              } while (cmdParams.loopEvery !== null);
            }),
          );
        },
        { appName: "beefy:run", logInfos: { msg: "beefy script", data: { task: argv.task, chains: argv.chain } } },
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
    case "reward-snapshots":
      return cmdParams.filterChains.map((chain) => () => importBeefyRewardSnapshots(chain, cmdParams));
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
  const emitError = (item: { product: DbProduct }) => {
    throw new Error(`Error fetching price feed for product ${item.product.productId}`);
  };

  const pipeline$ = productList$(cmdParams.client, "beefy", null).pipe(
    productFilter$(null, cmdParams),

    Rx.map((product) => ({ product })),

    // now fetch the price feed we need
    fetchPriceFeed$({
      ctx,
      emitError,
      getPriceFeedId: (item) => item.product.priceFeedId2,
      formatOutput: (item, priceFeed2) => ({ ...item, priceFeed2 }),
    }),
    fetchPriceFeed$({
      ctx,
      emitError,
      getPriceFeedId: (item) => item.product.pendingRewardsPriceFeedId,
      formatOutput: (item, rewardPriceFeed) => ({ ...item, rewardPriceFeed }),
    }),
    Rx.concatMap((item) =>
      [item.priceFeed2, item.rewardPriceFeed].filter((x): x is DbPriceFeed => !!x).map((priceFeed) => ({ product: item.product, priceFeed })),
    ),

    // remove duplicates
    Rx.distinct((item) => item.priceFeed.priceFeedId),

    // now import data for those
    cmdParams.task === "recent-prices"
      ? importBeefyRecentUnderlyingPrices$({ client: cmdParams.client })
      : importBeefyHistoricalUnderlyingPrices$({ client: cmdParams.client }),
  );

  logger.info({ msg: "starting share rate data import", data: { ...cmdParams, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

const historicalPipelineByChain = {} as Record<Chain, Rx.OperatorFunction<DbBeefyProduct, any>>;
const recentPipelineByChain = {} as Record<Chain, Rx.OperatorFunction<DbBeefyProduct, any>>;
function getChainInvestmentPipeline(chain: Chain, cmdParams: CmdParams, mode: "historical" | "recent") {
  if (mode === "historical") {
    if (!historicalPipelineByChain[chain]) {
      historicalPipelineByChain[chain] = importChainHistoricalData$({
        client: cmdParams.client,
        chain,
        forceCurrentBlockNumber: cmdParams.forceCurrentBlockNumber,
        forceRpcUrl: cmdParams.forceRpcUrl,
        rpcCount: cmdParams.rpcCount,
      });
    }
    return historicalPipelineByChain[chain];
  } else {
    if (!recentPipelineByChain[chain]) {
      recentPipelineByChain[chain] = importChainRecentData$({
        client: cmdParams.client,
        chain,
        forceCurrentBlockNumber: cmdParams.forceCurrentBlockNumber,
        forceRpcUrl: cmdParams.forceRpcUrl,
        rpcCount: cmdParams.rpcCount,
      });
    }
    return recentPipelineByChain[chain];
  }
  throw new ProgrammerError({ msg: "Unknown mode", data: { mode } });
}

async function importInvestmentData(chain: Chain, cmdParams: CmdParams) {
  const pipeline$ = productList$(cmdParams.client, "beefy", chain).pipe(
    productFilter$(chain, cmdParams),
    cmdParams.task === "recent" ? getChainInvestmentPipeline(chain, cmdParams, "recent") : getChainInvestmentPipeline(chain, cmdParams, "historical"),
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
    importBeefyHistoricalShareRatePrices$({
      chain: chain,
      client: cmdParams.client,
      forceCurrentBlockNumber: cmdParams.forceCurrentBlockNumber,
      forceRpcUrl: cmdParams.forceRpcUrl,
      rpcCount: cmdParams.rpcCount,
    }),
  );

  logger.info({ msg: "starting share rate data import", data: { ...cmdParams, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

function importBeefyRewardSnapshots(chain: Chain, cmdParams: CmdParams) {
  const rpcConfig = createRpcConfig(chain);
  const streamConfig = defaultHistoricalStreamConfig;
  const ctx = {
    chain,
    client: cmdParams.client,
    rpcConfig,
    streamConfig,
  };

  const emitError = (item: DbProduct) => {
    throw new Error(`Error fetching rewards for product ${item.productId}`);
  };

  const pipeline$ = productList$(cmdParams.client, "beefy", chain).pipe(
    productFilter$(chain, cmdParams),
    // Rewards only exists for boosts and governance vaults
    Rx.filter((product): product is DbBeefyBoostProduct | DbBeefyGovVaultProduct => isBeefyBoost(product) || isBeefyGovVault(product)),
    // fetch all investors of this product
    fetchAllInvestorIds$({
      ctx,
      emitError,
      getProductId: (product) => product.productId,
      formatOutput: (product, investorIds) => ({ product, investorIds }),
    }),
    // flatten the result
    Rx.concatMap(({ product, investorIds }) => investorIds.map((investorId) => ({ product, investorId }))),
    // fetch investor rows
    fetchInvestor$({
      ctx,
      emitError: (item) => {
        throw new Error(`Error fetching investor ${item.investorId} for product ${item.product.productId}`);
      },
      getInvestorId: (item) => item.investorId,
      formatOutput: (item, investor) => ({ ...item, investor }),
    }),

    // now fetch associated price feeds
    importBeefyHistoricalPendingRewardsSnapshots$({
      chain: chain,
      client: cmdParams.client,
      forceCurrentBlockNumber: cmdParams.forceCurrentBlockNumber,
      forceRpcUrl: cmdParams.forceRpcUrl,
      rpcCount: cmdParams.rpcCount,
    }),
  );

  logger.info({ msg: "starting pending rewards import", data: { ...cmdParams, client: "<redacted>" } });
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
      const contractAddress = getProductContractAddress(product);
      return cmdParams.filterContractAddress === null || contractAddress.toLocaleLowerCase() === cmdParams.filterContractAddress.toLocaleLowerCase();
    }),
  );
}

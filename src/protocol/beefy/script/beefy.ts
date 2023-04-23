import { cloneDeep, isNumber } from "lodash";
import * as Rx from "rxjs";
import yargs from "yargs";
import { Chain, allChainIds } from "../../../types/chain";
import { SamplingPeriod, allSamplingPeriods } from "../../../types/sampling";
import { DbClient, withDbClient } from "../../../utils/db";
import { mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { addSecretsToRpcUrl } from "../../../utils/rpc/remove-secrets-from-rpc-url";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { fetchAllInvestorIds$ } from "../../common/loader/investment";
import { fetchInvestor$ } from "../../common/loader/investor";
import { DbPriceFeed, fetchPriceFeed$ } from "../../common/loader/price-feed";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbProduct, productList$ } from "../../common/loader/product";
import { ErrorReport, ImportBehaviour, ImportCtx, createBatchStreamConfig, defaultImportBehaviour } from "../../common/types/import-context";
import { isProductDashboardEOL } from "../../common/utils/eol";
import { createRpcConfig } from "../../common/utils/rpc-config";
import { createBeefyIgnoreAddressRunner } from "../loader/ignore-address";
import { createBeefyHistoricalInvestmentRunner, createBeefyRecentInvestmentRunner } from "../loader/investment/import-investments";
import { createBeefyHistoricalPendingRewardsSnapshotsRunner } from "../loader/investment/import-pending-rewards-snapshots";
import { createBeefyInvestorCacheRunner } from "../loader/investor-cache-prices";
import { createBeefyHistoricalShareRatePricesRunner, createBeefyRecentShareRatePricesRunner } from "../loader/prices/import-share-rate-prices";
import { createBeefyHistoricalUnderlyingPricesRunner, createBeefyRecentUnderlyingPricesRunner } from "../loader/prices/import-underlying-prices";
import { createBeefyProductRunner } from "../loader/products";
import { getProductContractAddress } from "../utils/contract-accessors";
import { isBeefyBoost, isBeefyGovVault, isBeefyStandardVault } from "../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "import-script" });

interface CmdParams {
  client: DbClient;
  rpcCount: number | "all";
  forceRpcUrl: string | null;
  forceGetLogsBlockSpan: number | null;
  task:
    | "historical"
    | "recent"
    | "products"
    | "ignore-address"
    | "recent-prices"
    | "historical-prices"
    | "recent-share-rate"
    | "historical-share-rate"
    | "reward-snapshots"
    | "investor-cache";
  filterChains: Chain[];
  includeEol: boolean;
  forceCurrentBlockNumber: number | null;
  filterContractAddress: string | null;
  productRefreshInterval: SamplingPeriod | null;
  loopEvery: SamplingPeriod | null;
  loopEveryRandomizeRatio: number;
  ignoreImportState: boolean;
  disableWorkConcurrency: boolean;
  generateQueryCount: number | null;
  skipRecentWindowWhenHistorical: "all" | "none" | "live" | "eol";
  waitForBlockPropagation: number | null;
}

export function addBeefyCommands<TOptsBefore>(yargs: yargs.Argv<TOptsBefore>) {
  return yargs
    .command({
      command: "beefy:reimport",
      describe: "Reimport a product range",
      builder: (yargs) =>
        yargs.options({
          chain: {
            choices: allChainIds,
            alias: "c",
            demand: true,
            default: "all",
            describe: "only import data for this chain",
          },
          task: {
            type: "string",
            choices: ["historical", "recent"],
            demand: true,
            alias: "t",
            describe: "what to run",
          },
          contractAddress: { type: "string", demand: true, alias: "a", describe: "only import data for this contract address" },
          forceRpcUrl: { type: "string", demand: false, alias: "f", describe: "force a specific RPC URL" },
          rpcCount: { type: "number", demand: false, alias: "r", describe: "how many RPCs to use" },
          fromBlock: { type: "number", demand: true, describe: "from block" },
          toBlock: { type: "number", demand: true, describe: "to block" },
        }),
      handler: (argv): Promise<any> =>
        withDbClient(
          async (client) => {
            //await db_migrate();

            logger.info("Starting import script", { argv });

            const fromBlock = argv.fromBlock;
            const toBlock = argv.toBlock;
            if (fromBlock > toBlock) {
              throw new ProgrammerError("fromBlock > toBlock");
            }

            // we use the minimum block span accross all rpcs, this is not super efficient
            // but will work for now until there is a way to query {from:to} block ranges
            const minBlockSpan = 100;
            const blockSpanToCover = toBlock - fromBlock;
            const queriesToGenerate = Math.ceil(blockSpanToCover / minBlockSpan);

            const cmdParams: CmdParams = {
              client,
              rpcCount: argv.rpcCount === undefined || isNaN(argv.rpcCount) ? "all" : argv.rpcCount ?? 0,
              task: argv.task as CmdParams["task"],
              includeEol: true,
              filterChains: [argv.chain] as Chain[],
              filterContractAddress: argv.contractAddress || null,
              forceCurrentBlockNumber: argv.toBlock + 1,
              forceRpcUrl: argv.forceRpcUrl ? addSecretsToRpcUrl(argv.forceRpcUrl) : null,
              forceGetLogsBlockSpan: minBlockSpan,
              productRefreshInterval: (argv.productRefreshInterval as SamplingPeriod) || null,
              loopEvery: null,
              loopEveryRandomizeRatio: 0,
              ignoreImportState: true,
              disableWorkConcurrency: true,
              generateQueryCount: queriesToGenerate,
              skipRecentWindowWhenHistorical: "none",
              waitForBlockPropagation: 0,
            };

            _verifyCmdParams(cmdParams, argv);

            const tasks = getTasksToRun(cmdParams);

            return Promise.all(tasks.map((task) => task()));
          },
          { appName: "beefy:run", logInfos: { msg: "beefy script", data: { task: argv.task, chains: argv.chain } } },
        )(),
    })
    .command({
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
          forceGetLogsBlockSpan: { type: "number", demand: false, alias: "s", describe: "force a specific block span for getLogs" },
          includeEol: { type: "boolean", demand: false, default: false, alias: "e", describe: "Include EOL products for some chain" },
          task: {
            choices: [
              "historical",
              "recent",
              "products",
              "ignore-address",
              "recent-prices",
              "historical-prices",
              "recent-share-rate",
              "historical-share-rate",
              "reward-snapshots",
              "investor-cache",
            ],
            demand: true,
            alias: "t",
            describe: "what to run",
          },
          rpcCount: { type: "number", demand: false, alias: "r", describe: "how many RPCs to use" },
          productRefreshInterval: {
            choices: allSamplingPeriods,
            demand: false,
            alias: "p",
            describe: "how often workers should refresh the product list and redispatch accross rpcs",
          },
          loopEvery: { choices: allSamplingPeriods, demand: false, alias: "l", describe: "repeat the task from time to time" },
          loopEveryRandomizeRatio: {
            type: "number",
            demand: false,
            alias: "L",
            default: 0.05, // set a default to 5% jitter
            describe:
              "Add a random delay to the loop, in the [0; `loopEveryRandomizeRatio` * `loopEvery`] range. It's expressed in % (between 0 and 1) and is used to avoid perfect synchronization with monitoring",
          },
          ignoreImportState: {
            type: "boolean",
            demand: false,
            default: false,
            alias: "i",
            describe: "ignore the existing import state when generating new queries",
          },
          disableWorkConcurrency: {
            type: "boolean",
            demand: false,
            default: false,
            alias: "C",
            describe: "disable concurrency for work",
          },
          generateQueryCount: {
            type: "number",
            demand: false,
            alias: "q",
            describe: "generate a specific number of queries",
          },
          skipRecentWindowWhenHistorical: {
            choices: ["all", "none", "live", "eol"],
            demand: false,
            default: "all",
            alias: "S",
            describe: "skip the recent window when running historical",
          },
          waitForBlockPropagation: {
            type: "number",
            demand: false,
            alias: "P",
            describe: "Don't query too recent blocks",
          },
        }),
      handler: (argv): Promise<any> =>
        withDbClient(
          async (client) => {
            //await db_migrate();

            logger.info("Starting import script", { argv });

            const cmdParams: CmdParams = {
              client,
              rpcCount: argv.rpcCount === undefined || isNaN(argv.rpcCount) ? "all" : argv.rpcCount ?? 0,
              task: argv.task as CmdParams["task"],
              includeEol: argv.includeEol,
              filterChains: argv.chain.includes("all") ? allChainIds : (argv.chain as Chain[]),
              filterContractAddress: argv.contractAddress || null,
              forceCurrentBlockNumber: argv.currentBlockNumber || null,
              forceRpcUrl: argv.forceRpcUrl ? addSecretsToRpcUrl(argv.forceRpcUrl) : null,
              forceGetLogsBlockSpan: argv.forceGetLogsBlockSpan || null,
              productRefreshInterval: (argv.productRefreshInterval as SamplingPeriod) || null,
              loopEvery: argv.loopEvery || null,
              loopEveryRandomizeRatio: argv.loopEveryRandomizeRatio,
              ignoreImportState: argv.ignoreImportState,
              disableWorkConcurrency: argv.disableWorkConcurrency,
              generateQueryCount: argv.generateQueryCount || null,
              skipRecentWindowWhenHistorical: argv.skipRecentWindowWhenHistorical as CmdParams["skipRecentWindowWhenHistorical"],
              waitForBlockPropagation: argv.waitForBlockPropagation || null,
            };

            _verifyCmdParams(cmdParams, argv);

            const tasks = getTasksToRun(cmdParams);

            return Promise.all(tasks.map((task) => task()));
          },
          { appName: "beefy:run", logInfos: { msg: "beefy script", data: { task: argv.task, chains: argv.chain } } },
        )(),
    });
}

function getTasksToRun(cmdParams: CmdParams) {
  logger.trace({ msg: "starting", data: { ...cmdParams, client: "<redacted>" } });

  switch (cmdParams.task) {
    case "historical":
    case "recent":
      return cmdParams.filterChains.map((chain) => () => importInvestmentData(chain, cmdParams));
    case "products":
      return [() => importProducts(cmdParams)];
    case "recent-prices":
      return [() => importBeefyDataPrices(cmdParams)];
    case "historical-prices":
      return [() => importBeefyDataPrices(cmdParams)];
    case "ignore-address":
      return [() => importIgnoreAddress(cmdParams)];
    case "recent-share-rate":
    case "historical-share-rate":
      return cmdParams.filterChains.map((chain) => () => importBeefyDataShareRate(chain, cmdParams));
    case "reward-snapshots":
      return cmdParams.filterChains.map((chain) => () => importBeefyRewardSnapshots(chain, cmdParams));
    case "investor-cache":
      return [() => importInvestorCache(cmdParams)];
    default:
      throw new ProgrammerError(`Unknown importer: ${cmdParams.task}`);
  }
}

async function importProducts(cmdParams: CmdParams) {
  const runner = createBeefyProductRunner({
    runnerConfig: {
      getInputs: async () => cmdParams.filterChains,
      client: cmdParams.client,
      behaviour: _createImportBehaviourFromCmdParams(cmdParams),
    },
    client: cmdParams.client,
  });

  return runner.run();
}

async function importInvestorCache(cmdParams: CmdParams) {
  const runner = createBeefyInvestorCacheRunner({
    runnerConfig: {
      client: cmdParams.client,
      getInputs: async () => [null],
      behaviour: _createImportBehaviourFromCmdParams(cmdParams),
    },
    client: cmdParams.client,
  });

  return runner.run();
}

async function importIgnoreAddress(cmdParams: CmdParams) {
  const runner = createBeefyIgnoreAddressRunner({
    runnerConfig: {
      client: cmdParams.client,
      getInputs: async () => cmdParams.filterChains,
      behaviour: _createImportBehaviourFromCmdParams(cmdParams),
    },
    client: cmdParams.client,
  });

  return runner.run();
}

function importBeefyDataPrices(cmdParams: CmdParams) {
  const behaviour = _createImportBehaviourFromCmdParams(cmdParams);
  async function getInputs() {
    const rpcConfig = createRpcConfig("bsc", behaviour); // never used
    const streamConfig = createBatchStreamConfig("bsc", behaviour);
    const ctx: ImportCtx = {
      chain: "bsc" as Chain, // not used here
      client: cmdParams.client,
      rpcConfig,
      streamConfig,
      behaviour,
    };
    const emitError = (item: { product: DbProduct }, report: ErrorReport) => {
      logger.error(mergeLogsInfos({ msg: "Error fetching price feed for product", data: { ...item } }, report.infos));
      logger.error(report.error);
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

      // collect
      Rx.toArray(),
    );

    const res = await consumeObservable(pipeline$);
    if (!res) {
      return [];
    }
    return res;
  }

  // now import data for those
  const runnerConfig = { client: cmdParams.client, getInputs, behaviour };
  const runner =
    cmdParams.task === "recent-prices"
      ? createBeefyRecentUnderlyingPricesRunner({ runnerConfig })
      : createBeefyHistoricalUnderlyingPricesRunner({ runnerConfig });

  return runner.run();
}

async function importInvestmentData(chain: Chain, cmdParams: CmdParams) {
  async function getInputs() {
    const pipeline$ = productList$(cmdParams.client, "beefy", chain).pipe(
      productFilter$(chain, cmdParams),
      // collect
      Rx.toArray(),
    );

    const res = await consumeObservable(pipeline$);
    if (!res) {
      return [];
    }
    return res;
  }

  // now import data for those
  const runnerConfig = {
    chain,
    getInputs,
    client: cmdParams.client,
    behaviour: _createImportBehaviourFromCmdParams(cmdParams),
  };

  const runner =
    runnerConfig.behaviour.mode === "recent"
      ? createBeefyRecentInvestmentRunner({
          chain,
          runnerConfig,
        })
      : createBeefyHistoricalInvestmentRunner({
          chain,
          runnerConfig,
        });

  return runner.run();
}

function importBeefyDataShareRate(chain: Chain, cmdParams: CmdParams) {
  const behaviour = _createImportBehaviourFromCmdParams(cmdParams);
  const rpcConfig = createRpcConfig(chain, behaviour);
  const streamConfig = createBatchStreamConfig(chain, behaviour);
  const ctx: ImportCtx = {
    chain,
    client: cmdParams.client,
    rpcConfig,
    streamConfig,
    behaviour,
  };

  const emitError = (item: DbProduct, report: ErrorReport) => {
    logger.error(mergeLogsInfos({ msg: "Error fetching price feed for product", data: { ...item } }, report.infos));
    logger.error(report.error);
    throw new Error(`Error fetching price feed for product ${item.productId}`);
  };

  async function getInputs() {
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
      // collect
      Rx.toArray(),
    );

    const res = await consumeObservable(pipeline$);
    if (!res) {
      return [];
    }
    return res;
  }

  // now import data for those
  const runnerConfig = { client: cmdParams.client, chain, getInputs, behaviour };
  const runner =
    behaviour.mode === "recent"
      ? createBeefyRecentShareRatePricesRunner({ chain, runnerConfig })
      : createBeefyHistoricalShareRatePricesRunner({ chain, runnerConfig });

  return runner.run();
}

function importBeefyRewardSnapshots(chain: Chain, cmdParams: CmdParams) {
  const behaviour = _createImportBehaviourFromCmdParams(cmdParams);
  const rpcConfig = createRpcConfig(chain, behaviour);
  const streamConfig = createBatchStreamConfig(chain, behaviour);
  const ctx: ImportCtx = {
    chain,
    client: cmdParams.client,
    rpcConfig,
    streamConfig,
    behaviour,
  };

  const emitError = (item: DbProduct, report: ErrorReport) => {
    logger.error(mergeLogsInfos({ msg: "Error fetching rewards for product", data: { ...item } }, report.infos));
    logger.error(report.error);
    throw new Error(`Error fetching rewards for product ${item.productId}`);
  };

  async function getInputs() {
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

      // collect
      Rx.toArray(),
    );

    const res = await consumeObservable(pipeline$);
    if (!res) {
      return [];
    }
    return res;
  }

  // now import data for those
  const runner = createBeefyHistoricalPendingRewardsSnapshotsRunner({
    chain: chain,
    runnerConfig: {
      getInputs,
      client: cmdParams.client,
      chain,
      behaviour,
    },
  });

  return runner.run();
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
      const isLiveProduct = !isProductDashboardEOL(product);
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

const defaultModeByTask: Record<CmdParams["task"], "recent" | "historical"> = {
  historical: "historical",
  recent: "recent",
  products: "recent",
  "ignore-address": "recent",
  "recent-prices": "recent",
  "historical-prices": "historical",
  "recent-share-rate": "recent",
  "historical-share-rate": "historical",
  "reward-snapshots": "historical",
  "investor-cache": "recent",
};

export function _createImportBehaviourFromCmdParams(cmdParams: CmdParams, forceMode?: "historical" | "recent"): ImportBehaviour {
  const behaviour = cloneDeep(defaultImportBehaviour);
  behaviour.mode = forceMode || defaultModeByTask[cmdParams.task];
  if (cmdParams.productRefreshInterval !== null) {
    behaviour.inputPollInterval = cmdParams.productRefreshInterval;
  }
  if (cmdParams.loopEvery !== null) {
    behaviour.repeatAtMostEvery = cmdParams.loopEvery;
  }
  if (cmdParams.loopEveryRandomizeRatio !== null) {
    behaviour.repeatJitter = cmdParams.loopEveryRandomizeRatio;
  }
  if (cmdParams.forceCurrentBlockNumber !== null) {
    behaviour.forceCurrentBlockNumber = cmdParams.forceCurrentBlockNumber;
  }
  if (cmdParams.forceGetLogsBlockSpan !== null) {
    behaviour.forceGetLogsBlockSpan = cmdParams.forceGetLogsBlockSpan;
  }
  if (cmdParams.forceRpcUrl) {
    behaviour.forceRpcUrl = cmdParams.forceRpcUrl;
  }
  if (cmdParams.ignoreImportState) {
    behaviour.ignoreImportState = true;
    behaviour.skipRecentWindowWhenHistorical = "none"; // make the import predictable
  }
  if (cmdParams.skipRecentWindowWhenHistorical) {
    behaviour.skipRecentWindowWhenHistorical = cmdParams.skipRecentWindowWhenHistorical;
  }
  if (cmdParams.disableWorkConcurrency) {
    behaviour.disableConcurrency = true;
  }
  if (cmdParams.generateQueryCount !== null) {
    behaviour.limitQueriesCountTo = {
      investment: cmdParams.generateQueryCount,
      price: cmdParams.generateQueryCount,
      shareRate: cmdParams.generateQueryCount,
      snapshot: cmdParams.generateQueryCount,
    };
  }

  if (cmdParams.waitForBlockPropagation !== null) {
    behaviour.waitForBlockPropagation = cmdParams.waitForBlockPropagation;
  }

  return behaviour;
}

function _verifyCmdParams(cmdParams: CmdParams, argv: any) {
  if (cmdParams.forceCurrentBlockNumber !== null && cmdParams.filterChains.length > 1) {
    throw new ProgrammerError({
      msg: "Cannot force current block number without a chain filter",
      data: { cmdParams: { ...cmdParams, client: "<redacted>" }, argv },
    });
  }
  if (cmdParams.forceRpcUrl !== null && cmdParams.filterChains.length > 1) {
    throw new ProgrammerError({
      msg: "Cannot force RPC URL without a chain filter",
      data: { cmdParams: { ...cmdParams, client: "<redacted>" }, argv },
    });
  }
  if (cmdParams.forceRpcUrl !== null && cmdParams.rpcCount !== 1) {
    throw new ProgrammerError({
      msg: "Cannot force RPC URL with multiple RPCs",
      data: { cmdParams: { ...cmdParams, client: "<redacted>" }, argv },
    });
  }
  if (cmdParams.filterContractAddress !== null && cmdParams.filterChains.length > 1) {
    throw new ProgrammerError({
      msg: "Cannot filter contract address without a chain filter",
      data: { cmdParams: { ...cmdParams, client: "<redacted>" }, argv },
    });
  }
  if (cmdParams.generateQueryCount !== null && defaultModeByTask[cmdParams.task] !== "historical") {
    throw new ProgrammerError({
      msg: "Cannot generate query count without historical mode",
      data: { cmdParams: { ...cmdParams, client: "<redacted>" }, argv },
    });
  }
  if (!isNumber(cmdParams.loopEveryRandomizeRatio) || cmdParams.loopEveryRandomizeRatio < 0 || cmdParams.loopEveryRandomizeRatio > 1) {
    throw new ProgrammerError({
      msg: "loopEveryRandomizeRatio should be a number between 0 and 1",
      data: { cmdParams: { ...cmdParams, client: "<redacted>" }, argv },
    });
  }
}

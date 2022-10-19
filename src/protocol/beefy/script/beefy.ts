import { PoolClient } from "pg";
import * as Rx from "rxjs";
import yargs from "yargs";
import { allChainIds, Chain } from "../../../types/chain";
import { allSamplingPeriods, SamplingPeriod, samplingPeriodMs } from "../../../types/sampling";
import { sleep } from "../../../utils/async";
import { db_migrate, withPgClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
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

interface CmdParams {
  client: PoolClient;
  task: "historical" | "recent" | "products" | "recent-prices" | "historical-prices" | "historical-share-rate";
  filterChains: Chain[];
  forceCurrentBlockNumber: number | null;
  filterContractAddress: string | null;
  sleepAfterImport: SamplingPeriod | null;
  repeat: boolean;
}

export function addBeefyCommands<TOptsBefore>(yargs: yargs.Argv<TOptsBefore>) {
  return yargs.command({
    command: "beefy:run",
    describe: "Start a single beefy import",
    builder: (yargs) =>
      yargs.options({
        chain: { choices: [...allChainIds, "all"], alias: "c", demand: false, default: "all", describe: "only import data for this chain" },
        contractAddress: { type: "string", demand: false, alias: "a", describe: "only import data for this contract address" },
        currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "Force the current block number" },
        task: {
          choices: ["historical", "recent", "products", "recent-prices", "historical-prices", "historical-share-rate"],
          demand: true,
          alias: "t",
          describe: "what to run",
        },
        sleepAfterImport: { choices: allSamplingPeriods, demand: false, alias: "s", describe: "sleep after import" },
        repeat: { type: "boolean", demand: false, alias: "r", default: false, describe: "repeat import when ended" },
      }),
    handler: (argv): Promise<any> =>
      withPgClient(async (client) => {
        //await db_migrate();

        logger.info("Starting import script", { argv });

        const cmdParams: CmdParams = {
          client,
          task: argv.task as CmdParams["task"],
          filterChains: argv.chain === "all" ? allChainIds : [argv.chain as Chain],
          filterContractAddress: argv.contractAddress || null,
          forceCurrentBlockNumber: argv.currentBlockNumber || null,
          sleepAfterImport: argv.sleepAfterImport || null,
          repeat: argv.repeat || false,
        };
        if (cmdParams.forceCurrentBlockNumber !== null && cmdParams.filterChains.length > 1) {
          throw new ProgrammerError("Cannot force current block number without a chain filter");
        }

        return runCmd(cmdParams).then(() => {
          logger.info("Import script finished");
          const sleepAfterImport = argv.sleepAfterImport as SamplingPeriod | undefined;
          if (sleepAfterImport) {
            logger.info({ msg: "Sleeping after import", data: { sleepAfterImport } });
            return sleep(samplingPeriodMs[sleepAfterImport]);
          }
        });
      })(),
  });
}

async function runCmd(cmdParams: CmdParams) {
  logger.trace({ msg: "starting", data: { ...cmdParams, client: "<redacted>" } });

  switch (cmdParams.task) {
    case "historical":
      return Promise.all(cmdParams.filterChains.map((chain) => importInvestmentData(chain, cmdParams)));
    case "recent":
      return Promise.all(cmdParams.filterChains.map((chain) => importInvestmentData(chain, cmdParams)));
    case "products":
      return importProducts(cmdParams);
    case "recent-prices":
      return importBeefyDataPrices(cmdParams);
    case "historical-prices":
      return importBeefyDataPrices(cmdParams);
    case "historical-share-rate":
      return Promise.all(cmdParams.filterChains.map((chain) => importBeefyDataShareRate(chain, cmdParams)));
    default:
      throw new ProgrammerError(`Unknown importer: ${cmdParams.task}`);
  }
}

async function importProducts(cmdParams: CmdParams) {
  const pipeline$ = Rx.from(cmdParams.filterChains).pipe(importBeefyProducts$({ client: cmdParams.client }));
  logger.info({ msg: "starting product list import", data: { ...cmdParams, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

async function importBeefyDataPrices(cmdParams: CmdParams) {
  const pipeline$ = priceFeedList$(cmdParams.client, "beefy-data").pipe(
    cmdParams.task === "recent-prices"
      ? importBeefyRecentUnderlyingPrices$({ client: cmdParams.client })
      : importBeefyHistoricalUnderlyingPrices$({ client: cmdParams.client }),
  );
  logger.info({ msg: "starting prices import", data: { ...cmdParams, client: "<redacted>" } });
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
    client: cmdParams.client,
    emitErrors: (item: DbProduct) => {
      throw new Error(`Error fetching price feed for product ${item.productId}`);
    },
    rpcConfig,
    streamConfig,
  };

  const pipeline$ = productList$(cmdParams.client, "beefy", chain).pipe(
    productFilter$(chain, cmdParams),
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
    importBeefyHistoricalShareRatePrices$({ chain: chain, ...cmdParams }),
  );

  logger.info({ msg: "starting share rate data import", data: { ...cmdParams, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

function productFilter$(chain: Chain, cmdParams: CmdParams) {
  return Rx.pipe(
    Rx.filter((product: DbProduct) => product.chain === chain),
    Rx.filter((product) => {
      const contractAddress =
        product.productData.type === "beefy:boost" ? product.productData.boost.contract_address : product.productData.vault.contract_address;
      return cmdParams.filterContractAddress === null || contractAddress.toLocaleLowerCase() === cmdParams.filterContractAddress.toLocaleLowerCase();
    }),
  );
}

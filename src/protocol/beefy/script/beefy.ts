import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { db_migrate, withPgClient } from "../../../utils/db";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger";
import { DbBeefyProduct, productList$ } from "../../common/loader/product";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { sleep } from "../../../utils/async";
import { importBeefyProducts$ } from "../loader/products";
import { importBeefyUnderlyingPrices$ } from "../loader/prices";
import { importChainHistoricalData$ } from "../loader/investment/import-investments";
import { ProgrammerError } from "../../../utils/programmer-error";
import yargs from "yargs";
import { priceFeedList$ } from "../../common/loader/price-feed";
import { samplingPeriodMs } from "../../../types/sampling";

const logger = rootLogger.child({ module: "beefy", component: "import-script" });

export function addBeefyCommands<TOptsBefore>(yargs: yargs.Argv<TOptsBefore>) {
  return yargs
    .command({
      command: "beefy:daemon",
      describe: "Start the beefy importer daemon",
      builder: (yargs) =>
        yargs.options({
          chain: { choices: [...allChainIds, "all"], alias: "c", demand: false, default: "all", describe: "only import data for this chain" },
        }),
      handler: async (argv) =>
        withPgClient(async (client) => {
          await db_migrate();

          logger.info("Starting import daemon", { argv });

          const chain = argv.chain as Chain | "all";
          const filterChains = chain === "all" ? allChainIds : [chain];
          const filterContractAddress = null;
          const forceCurrentBlockNumber = null;

          if (forceCurrentBlockNumber !== null && chain === "all") {
            throw new ProgrammerError("Cannot force current block number without a chain filter");
          }

          logger.trace({ msg: "starting", data: { filterChains, filterContractAddress } });

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

          return new Promise<any>(async () => {
            for (const chain of filterChains) {
              daemonize(
                `investment-recent-${chain}`,
                () => importInvestmentData({ client, forceCurrentBlockNumber, strategy: "recent", chain, filterContractAddress }),
                samplingPeriodMs["1min"],
              );
              daemonize(
                `investment-historical-${chain}`,
                () => importInvestmentData({ client, forceCurrentBlockNumber, strategy: "historical", chain, filterContractAddress }),
                samplingPeriodMs["5min"],
              );
            }

            daemonize("prices", () => importBeefyDataPrices({ client }), samplingPeriodMs["15min"]);
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
            choices: ["historical", "recent", "products", "prices"],
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
            case "prices":
              return importBeefyDataPrices({ client });
            default:
              throw new ProgrammerError(`Unknown importer: ${argv.importer}`);
          }
        })(),
    });
}

async function importProducts(options: { client: PoolClient; filterChains: Chain[] }) {
  const pipeline$ = Rx.from(options.filterChains).pipe(importBeefyProducts$({ client: options.client }));
  logger.info({ msg: "starting product list import", data: { ...options, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

async function importBeefyDataPrices(options: { client: PoolClient }) {
  const pipeline$ = priceFeedList$(options.client, "beefy-data").pipe(importBeefyUnderlyingPrices$({ client: options.client }));
  logger.info({ msg: "starting prices import", data: { ...options, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

const investmentPipelineByChain = {} as Record<
  Chain,
  { historical: Rx.OperatorFunction<DbBeefyProduct, any>; recent: Rx.OperatorFunction<DbBeefyProduct, any> }
>;
function getChainInvestmentPipeline(client: PoolClient, chain: Chain, filterContractAddress: string | null, forceCurrentBlockNumber: number | null) {
  if (!investmentPipelineByChain[chain]) {
    investmentPipelineByChain[chain] = {
      historical: Rx.pipe(
        Rx.filter((product) => product.chain === chain),
        Rx.filter(
          (product) =>
            filterContractAddress === null ||
            (product.productData.type === "beefy:boost"
              ? product.productData.boost.contract_address.toLocaleLowerCase() === filterContractAddress.toLocaleLowerCase()
              : product.productData.vault.contract_address.toLocaleLowerCase() === filterContractAddress.toLocaleLowerCase()),
        ),
        importChainHistoricalData$(client, chain, forceCurrentBlockNumber),
      ),
      recent: Rx.pipe(
        Rx.filter((product) => product.chain === chain),
        Rx.filter(
          (product) =>
            filterContractAddress === null ||
            (product.productData.type === "beefy:boost"
              ? product.productData.boost.contract_address.toLocaleLowerCase() === filterContractAddress.toLocaleLowerCase()
              : product.productData.vault.contract_address.toLocaleLowerCase() === filterContractAddress.toLocaleLowerCase()),
        ),
        importChainHistoricalData$(client, chain, forceCurrentBlockNumber),
      ),
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
  const chainPipeline = getChainInvestmentPipeline(options.client, options.chain, options.filterContractAddress, options.forceCurrentBlockNumber);
  const strategyImporter = options.strategy === "recent" ? chainPipeline.recent : chainPipeline.historical;
  const pipeline$ = productList$(options.client, "beefy").pipe(strategyImporter);
  logger.info({ msg: "starting investment data import", data: { ...options, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

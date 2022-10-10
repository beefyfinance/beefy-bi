import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { db_migrate, withPgClient } from "../../../utils/db";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger";
import { loaderByChain$ } from "../../common/loader/loader-by-chain";
import { DbProduct, productList$ } from "../../common/loader/product";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { sleep } from "../../../utils/async";
import { importBeefyProducts$ } from "../loader/products";
import { importBeefyPrices$ } from "../loader/prices";
import { importChainHistoricalData$, importChainRecentData$ } from "../loader/investment/import-investments";
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
      handler: async (argv) => {
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

        return new Promise<any>(async () => {
          (async () => {
            while (true) {
              await importProducts({ filterChains });
              await sleep(samplingPeriodMs["1day"]);
            }
          })();
          for (const chain of allChainIds) {
            if (!filterChains.includes(chain)) {
              continue;
            }
            (async () => {
              while (true) {
                await importInvestmentData({ forceCurrentBlockNumber, strategy: "recent", filterChains: [chain], filterContractAddress });
                await sleep(samplingPeriodMs["1min"]);
              }
            })();
            (async () => {
              while (true) {
                await importInvestmentData({ forceCurrentBlockNumber, strategy: "historical", filterChains: [chain], filterContractAddress });
                await sleep(samplingPeriodMs["5min"]);
              }
            })();
          }
          (async () => {
            while (true) {
              await importPrices();
              await sleep(samplingPeriodMs["15min"]);
            }
          })();
        });
      },
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
      handler: (argv): Promise<any> => {
        logger.info("Starting import script", { argv });

        const chain = argv.chain as Chain | "all";
        const filterChains = chain === "all" ? allChainIds : [chain];
        const filterContractAddress = argv.contractAddress || null;
        const forceCurrentBlockNumber = argv.currentBlockNumber || null;

        if (forceCurrentBlockNumber !== null && chain === "all") {
          throw new ProgrammerError("Cannot force current block number without a chain filter");
        }

        logger.trace({ msg: "starting", data: { filterChains, filterContractAddress } });

        switch (argv.importer) {
          case "historical":
            return importInvestmentData({ forceCurrentBlockNumber, strategy: "historical", filterChains, filterContractAddress });
          case "recent":
            return importInvestmentData({ forceCurrentBlockNumber, strategy: "recent", filterChains, filterContractAddress });
          case "products":
            return importProducts({ filterChains });
          case "prices":
            return importPrices();
          default:
            throw new ProgrammerError(`Unknown importer: ${argv.importer}`);
        }
      },
    });
}

async function importProducts(options: { filterChains: Chain[] }) {
  return withPgClient((client) => {
    const pipeline$ = Rx.of(...allChainIds).pipe(
      Rx.filter((chain) => options.filterChains.includes(chain)),
      importBeefyProducts$({ client }),
    );
    return consumeObservable(pipeline$);
  })();
}

async function importPrices() {
  return withPgClient((client) => {
    const pipeline$ = priceFeedList$(client, "beefy").pipe(importBeefyPrices$({ client }));
    return consumeObservable(pipeline$);
  })();
}

async function importInvestmentData(options: {
  forceCurrentBlockNumber: number | null;
  strategy: "recent" | "historical";
  filterChains: Chain[];
  filterContractAddress: string | null;
}) {
  const strategyImporter = options.strategy === "recent" ? importChainRecentData$ : importChainHistoricalData$;

  return withPgClient(async (client: PoolClient) => {
    const process = (chain: Chain) => (input$: Rx.Observable<DbProduct>) =>
      input$.pipe(strategyImporter(client, chain, options.forceCurrentBlockNumber));

    const pipeline$ = productList$(client, "beefy").pipe(
      // apply command filters
      Rx.filter((product) => options.filterChains.includes(product.chain)),
      Rx.filter(
        (product) =>
          options.filterContractAddress === null ||
          (product.productData.type === "beefy:vault"
            ? product.productData.vault.contract_address.toLocaleLowerCase() === options.filterContractAddress.toLocaleLowerCase()
            : product.productData.boost.contract_address.toLocaleLowerCase() === options.filterContractAddress.toLocaleLowerCase()),
      ),

      // load  historical data
      loaderByChain$(process),
    );
    return consumeObservable(pipeline$);
  })();
}

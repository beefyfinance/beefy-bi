import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { withPgClient } from "../../../utils/db";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger";
import { loaderByChain$ } from "../../common/loader/loader-by-chain";
import { DbProduct, productList$ } from "../../common/loader/product";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { sleep } from "../../../utils/async";
import { importBeefyProducts$ } from "../loader/products";
import { importBeefyPrices$ } from "../loader/prices";
import { importChainHistoricalData$, importChainRecentData$ } from "../loader/investment/import-investments";
import { ProgrammerError } from "../../../utils/rxjs/utils/programmer-error";
import yargs from "yargs";
import { priceFeedList$ } from "../../common/loader/price-feed";
import { samplingPeriodMs } from "../../../types/sampling";

const logger = rootLogger.child({ module: "beefy", component: "import-script" });

export function addBeefyCommands<TOptsBefore>(yargs: yargs.Argv<TOptsBefore>) {
  return yargs.command(
    "beefy daemon",
    "Start the beefy importer daemon",
    (yargs) =>
      yargs.options({
        chain: { choices: [...allChainIds, "all"], alias: "c", demand: false, default: "all", describe: "only import data for this chain" },
        contractAddress: { type: "string", demand: false, alias: "a", describe: "only import data for this contract address" },
        currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "Force the current block number" },
        run: {
          choices: ["historical-investments", "live-investments", "products", "prices"],
          demand: false,
          alias: "r",
          describe: "what to run, all if not specified",
        },
      }),
    async (argv) => {
      logger.info("Starting import script", { argv });

      const chain = argv.chain as Chain | "all";
      const filterChains = chain === "all" ? allChainIds : [chain];
      const filterContractAddress = argv.contractAddress || null;
      const forceCurrentBlockNumber = argv.currentBlockNumber || null;

      if (forceCurrentBlockNumber !== null && chain === "all") {
        throw new ProgrammerError("Cannot force current block number without a chain filter");
      }

      logger.trace({ msg: "starting", data: { filterChains, filterContractAddress } });

      return new Promise(async () => {
        // if not defined, run as a daemon
        if (argv.run === undefined) {
          (async () => {
            while (true) {
              await importProducts();
              await sleep(samplingPeriodMs["1day"]);
            }
          })();
          (async () => {
            while (true) {
              await importInvestmentData({ forceCurrentBlockNumber, strategy: "live", filterChains, filterContractAddress });
              await sleep(samplingPeriodMs["1min"]);
            }
          })();
          (async () => {
            while (true) {
              await importInvestmentData({ forceCurrentBlockNumber, strategy: "historical", filterChains, filterContractAddress });
              await sleep(samplingPeriodMs["1hour"]);
            }
          })();
          (async () => {
            while (true) {
              await importPrices();
              await sleep(samplingPeriodMs["15min"]);
            }
          })();
        } else {
          // otherwise run the specified command
          switch (argv.run) {
            case "historical-investments":
              await importInvestmentData({ forceCurrentBlockNumber, strategy: "historical", filterChains, filterContractAddress });
              break;
            case "live-investments":
              await importInvestmentData({ forceCurrentBlockNumber, strategy: "live", filterChains, filterContractAddress });
              break;
            case "products":
              await importProducts();
              break;
            case "prices":
              await importPrices();
              break;
          }
        }
      });
    },
  );
}

async function importProducts() {
  return withPgClient((client) => {
    const pipeline$ = Rx.of(...allChainIds).pipe(importBeefyProducts$({ client }));
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
  strategy: "live" | "historical";
  filterChains: Chain[];
  filterContractAddress: string | null;
}) {
  const strategyImporter = options.strategy === "live" ? importChainRecentData$ : importChainHistoricalData$;

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

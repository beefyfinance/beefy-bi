import { _fetchContractFirstLastTrx } from "../lib/contract-transaction-infos";
import { allChainIds } from "../types/chain";
import { logger } from "../utils/logger";
import yargs from "yargs";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: allChainIds, alias: "c", demand: true },
    }).argv;

  const chain = argv.chain;

  logger.info(`Importing ${chain} block samples.`);

  // create a timeserie of blocks as a csv with columns blockNumber;dateTime
  // we don't want all block, just a somewhat regular sampled timeserie (15min)
  // dir structure: /<chain>/blocks/samples/15min.csv
}

main()
  .then(() => {
    logger.info("Done");
    process.exit(0);
  })
  .catch((e) => {
    console.log(e);
    logger.error(e);
    process.exit(1);
  });

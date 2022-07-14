import { estimateMsPerBlock } from "../import/import-block-samples";
import { allChainIds } from "../types/chain";
import { runMain } from "../utils/process";

import yargs from "yargs";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: allChainIds, alias: "c", demand: true },
    }).argv;

  const res = await estimateMsPerBlock(argv.chain);
  console.log(res);
}

runMain(main);

import { runMain } from "../utils/process";

import yargs from "yargs";
import { normalizeAddress } from "../utils/ethers";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      address: { type: "string", alias: "a", demand: true },
    }).argv;

  const res = normalizeAddress(argv.address);
  console.log(res);
}

runMain(main);

import yargs from "yargs";
import { allChainIds } from "../types/chain";
import { runMain } from "../utils/process";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .scriptName("runner")
    .usage("$0 <cmd> [args]")
    .command(
      "beefy ",
      "welcome ter yargs!",
      (yargs) => {
        yargs.options({
          chain: { choices: [...allChainIds, "all"], alias: "c", demand: false, default: "all", describe: "only import data for this chain" },
          contractAddress: { type: "string", demand: false, alias: "a", describe: "only import data for this contract address" },
          currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "Force the current block number" },
        });
      },
      function (argv) {
        console.log("hello", argv.name, "welcome to yargs!");
      },
    );
}

runMain(main);

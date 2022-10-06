import yargs from "yargs";
import { addBeefyCommands } from "../protocol/beefy/script/beefy";
import { runMain } from "../utils/process";

async function main() {
  const baseCmd = yargs.scriptName("runner").usage("$0 <cmd> [args]");

  const cmd = addBeefyCommands(baseCmd);

  return cmd.argv; // this starts the command for some reason
}

runMain(main);

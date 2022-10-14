import yargs from "yargs";
import { addBeefyCommands } from "../protocol/beefy/script/beefy";
import { db_migrate } from "../utils/db";
import { runMain } from "../utils/process";

async function main() {
  const baseCmd = yargs.usage("$0 <cmd> [args]");

  let cmd = addBeefyCommands(baseCmd);

  // add a migrate command
  cmd = yargs.command({
    command: "db:migrate",
    describe: "run db migrations",
    handler: db_migrate,
  });

  return cmd.demandCommand().help().argv; // this starts the command for some reason
}

runMain(main);

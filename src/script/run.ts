import yargs from "yargs";
import { addBeefyCommands } from "../protocol/beefy/script/beefy";
import { db_migrate, withDbClient } from "../utils/db";
import { runMain } from "../utils/process";

async function main() {
  let cmd = yargs.usage("$0 <cmd> [args]");

  cmd = addBeefyCommands(cmd);

  // add a migrate command
  cmd = yargs.command({
    command: "db:migrate",
    describe: "run db migrations",
    handler: withDbClient(db_migrate, { appName: "beefy:db:migrate", logInfos: { msg: "db:migrate" }, connectTimeoutMs: 10000 }),
  });

  return cmd.demandCommand().help().argv; // this starts the command for some reason
}

runMain(main);

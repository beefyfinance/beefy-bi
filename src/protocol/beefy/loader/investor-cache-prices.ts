import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { DbClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { createChainRunner, NoRpcRunnerConfig } from "../../common/utils/rpc-chain-runner";
import { addMissingInvestorCacheUsdInfos } from "./investment/investor-cache";

const logger = rootLogger.child({ module: "beefy", component: "import-investor-cache" });

export function createBeefyInvestorCacheRunner(options: { client: DbClient; runnerConfig: NoRpcRunnerConfig<null> }) {
  return createChainRunner(options.runnerConfig, (ctx) =>
    Rx.pipe(
      Rx.concatMap(async (...args) => {
        console.log({ args });
        await addMissingInvestorCacheUsdInfos({ client: options.client });
        logger.info("Done");
      }),
    ),
  );
}

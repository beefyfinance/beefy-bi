import * as Rx from "rxjs";
import { DbClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { createChainRunner, NoRpcRunnerConfig } from "../../common/utils/rpc-chain-runner";
import { addMissingInvestorCacheUsdInfos$ } from "./investment/investor-cache";

const logger = rootLogger.child({ module: "beefy", component: "import-investor-cache" });

export function createBeefyInvestorCacheRunner(options: { client: DbClient; runnerConfig: NoRpcRunnerConfig<null> }) {
  return createChainRunner(options.runnerConfig, (ctx) =>
    Rx.pipe(
      addMissingInvestorCacheUsdInfos$({ ctx }),
      Rx.tap(() => logger.info("imported investor cache")),
    ),
  );
}

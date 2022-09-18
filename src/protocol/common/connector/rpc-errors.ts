import * as Rx from "rxjs";
import { isErrorDueToMissingDataFromNode } from "../../../lib/rpc/archive-node-needed";
import { rootLogger } from "../../../utils/logger";
import { retryRpcErrors } from "../../../utils/rxjs/utils/retry-rpc";

const logger = rootLogger.child({ module: "rpc-errors" });

export function handleRpcErrors<TInput>(logInfos: { msg: string; data: object }): Rx.OperatorFunction<TInput, TInput> {
  return ($source) =>
    $source.pipe(
      // retry some errors
      retryRpcErrors(logInfos),

      // or catch the rest
      Rx.catchError((error) => {
        // don't show the full stack trace if we know what's going on
        if (isErrorDueToMissingDataFromNode(error)) {
          logger.error({ msg: `need archive node for ${logInfos.msg}`, data: logInfos.data });
        } else {
          logger.error({ msg: "error mapping token balance", data: { ...logInfos, error } });
          logger.error(error);
        }
        return Rx.EMPTY;
      }),
    );
}

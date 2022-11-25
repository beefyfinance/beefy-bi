import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { DbClient } from "../../../utils/db";
import { LogInfos } from "../../../utils/logger";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";

type Throwable = Error | string;

export type ErrorReport = {
  previousError?: ErrorReport;
  error?: Throwable;
  infos: LogInfos;
};

export type ErrorEmitter<T> = (obj: T, report: ErrorReport) => void;

export interface ImportCtx {
  client: DbClient;
  streamConfig: BatchStreamConfig;
  // sometimes we don't need it, but it's simpler to pass it everywhere
  chain: Chain;
  rpcConfig: RpcConfig;
}

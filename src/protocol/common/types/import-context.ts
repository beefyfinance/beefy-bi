import { RpcConfig } from "../../../types/rpc-config";
import { DbClient } from "../../../utils/db";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";

export interface ImportCtx<TTarget> {
  client: DbClient;
  streamConfig: BatchStreamConfig;
  emitErrors: (item: TTarget) => void;
  // sometimes we don't need it, but it's simpler to pass it everywhere
  rpcConfig: RpcConfig;
}

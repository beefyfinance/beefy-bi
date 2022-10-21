import { PoolClient } from "pg";
import { RpcConfig } from "../../../types/rpc-config";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";

export interface ImportCtx<TTarget> {
  client: PoolClient;
  streamConfig: BatchStreamConfig;
  emitErrors: (item: TTarget) => void;
  // sometimes we don't need it, but it's simpler to pass it everywhere
  rpcConfig: RpcConfig;
}

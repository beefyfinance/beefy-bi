import { PoolClient } from "pg";
import { RpcConfig } from "../../../types/rpc-config";
import { SupportedRangeTypes } from "../../../utils/range";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { ErrorEmitter } from "./import-query";

export interface ImportCtx<TTarget> {
  client: PoolClient;
  streamConfig: BatchStreamConfig;
  emitErrors: (item: TTarget) => void;

  // sometimes we don't need it, but it's simpler to pass it everywhere
  rpcConfig: RpcConfig;
}

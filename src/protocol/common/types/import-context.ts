import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { DbClient } from "../../../utils/db";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";

export type ErrorEmitter<T> = (obj: T) => void;

export interface ImportCtx {
  client: DbClient;
  streamConfig: BatchStreamConfig;
  // sometimes we don't need it, but it's simpler to pass it everywhere
  chain: Chain;
  rpcConfig: RpcConfig;
}

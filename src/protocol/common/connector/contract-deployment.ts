import { rootLogger } from "../../../utils/logger";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ImportCtx } from "../types/import-context";
import { findFirstBlockWithCode } from "../utils/first-block-with-code";

const logger = rootLogger.child({ module: "common", component: "contract-deployment" });

export interface ContractDeploymentInfos {
  blockNumber: number;
  datetime: Date;
}

/**
 * Find the deployment block of a contract from the chain itself: the first block where it has code.
 * Works without any explorer or API. Costs about log2(latest block) + 2 rpc calls, and needs an archive node.
 */
export async function getContractDeploymentInfos(ctx: ImportCtx, contractAddress: string): Promise<ContractDeploymentInfos | null> {
  const provider = ctx.rpcConfig.linearProvider;
  const callOptions = (msg: string) => ({
    logInfos: { msg, data: { chain: ctx.chain, contractAddress } },
    chain: ctx.chain,
    provider,
    rpcLimitations: ctx.rpcConfig.rpcLimitations,
    maxTotalRetryMs: 60 * 1000,
    noLockIfNoLimit: true,
  });

  const latestBlockNumber = await callLockProtectedRpc(() => provider.getBlockNumber(), callOptions("deployment: latest block number"));
  const blockNumber = await findFirstBlockWithCode({
    latestBlockNumber,
    hasCodeAt: async (n) => {
      const code = await callLockProtectedRpc(() => provider.getCode(contractAddress, n), callOptions("deployment: get code"));
      return code !== "0x";
    },
  });
  if (blockNumber === null) {
    logger.warn({ msg: "No code found for contract at latest block", data: { chain: ctx.chain, contractAddress } });
    return null;
  }

  // fetch the block so that its number and datetime match
  const block = await callLockProtectedRpc(() => provider.getBlock(blockNumber), callOptions("deployment: block"));
  logger.info({ msg: "Found contract deployment block from chain code", data: { chain: ctx.chain, contractAddress, blockNumber } });
  return { blockNumber: block.number, datetime: new Date(block.timestamp * 1000) };
}

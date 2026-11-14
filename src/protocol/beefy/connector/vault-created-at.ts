import axios from "axios";
import { Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ImportCtx } from "../../common/types/import-context";
import { BlockRef, BlockTimeChange, BlockTimeModel, findBlockNearTimestamp } from "../../common/utils/block-at-timestamp";

const logger = rootLogger.child({ module: "beefy", component: "vault-created-at" });

const BEEFY_VAULTS_API_URL = "https://api.beefy.finance/vaults/all";

// createdAt is when the vault was listed on Beefy, which can be well after its contract was deployed.
// we start the import this long before createdAt to make sure we don't miss the first deposits.
export const VAULT_CREATED_AT_BUFFER_SECONDS = 30 * 24 * 60 * 60;

interface BeefyApiVault {
  chain: string;
  earnContractAddress: string;
  createdAt: number; // unix timestamp in seconds
}

interface VaultFallbackCreationInfos {
  blockNumber: number;
  datetime: Date;
  createdAt: number;
}

const getKey = (chain: string, contractAddress: string) => `${chain}:${contractAddress.toLowerCase()}`;

// fetched once per process, a failed fetch is not cached so that the next call retries it
let createdAtMapPromise: Promise<Map<string, number>> | null = null;
function getVaultCreatedAtMap(): Promise<Map<string, number>> {
  if (!createdAtMapPromise) {
    const promise = (async () => {
      const resp = await axios.get<BeefyApiVault[]>(BEEFY_VAULTS_API_URL);
      const map = new Map<string, number>();
      for (const vault of resp.data) {
        if (vault.chain && vault.earnContractAddress && vault.createdAt) {
          map.set(getKey(vault.chain, vault.earnContractAddress), vault.createdAt);
        }
      }
      logger.info({ msg: "Fetched beefy vaults createdAt from api", data: { url: BEEFY_VAULTS_API_URL, count: map.size } });
      return map;
    })();
    createdAtMapPromise = promise;
    promise.catch(() => {
      if (createdAtMapPromise === promise) {
        createdAtMapPromise = null;
      }
    });
  }
  return createdAtMapPromise;
}

// how far back we sample the chain to measure its current average block time
// short enough to stay after a recent block time change, long enough to average out timestamp rounding
const BLOCK_TIME_SAMPLE_BLOCKS = 100_000;

// chains that changed block time, the rate before the change is not measured live
const BLOCK_TIME_CHANGES: Partial<Record<Chain, BlockTimeChange>> = {
  // Base Denim: 200ms blocks from 2026-11-05 18:00 UTC, 2s blocks before
  // https://docs.base.org/upgrades/denim/200ms-blocks
  base: { timestamp: Date.UTC(2026, 10, 5, 18, 0, 0) / 1000, secondsPerBlockBefore: 2 },
};

// how close the estimated block must be to the target, the buffer is 30 days so a day is plenty
const CREATION_BLOCK_TOLERANCE_SECONDS = 24 * 60 * 60;
// correction probes after the first estimate
const CREATION_BLOCK_MAX_CORRECTIONS = 2;

/**
 * Used when the explorer could not give us the contract creation block.
 * Takes the vault createdAt from the beefy api and goes back bufferSeconds.
 * The block is estimated, not searched exactly: the buffer already covers the imprecision.
 * The current block time is measured on the chain, and known block time changes (see BLOCK_TIME_CHANGES) are applied.
 * Costs 2 rpc calls to measure the block time, then 1 to 3 calls for the block itself.
 */
export async function getVaultFallbackCreationInfos(
  ctx: ImportCtx,
  contractAddress: string,
  bufferSeconds: number = VAULT_CREATED_AT_BUFFER_SECONDS,
): Promise<VaultFallbackCreationInfos | null> {
  const createdAtMap = await getVaultCreatedAtMap();
  const createdAt = createdAtMap.get(getKey(ctx.chain, contractAddress));
  if (!createdAt) {
    logger.warn({ msg: "Vault not found in beefy api, no fallback creation date", data: { chain: ctx.chain, contractAddress } });
    return null;
  }

  const targetTimestamp = createdAt - bufferSeconds;
  const provider = ctx.rpcConfig.linearProvider;
  const callOptions = (msg: string) => ({
    logInfos: { msg, data: { chain: ctx.chain, contractAddress } },
    chain: ctx.chain,
    provider,
    rpcLimitations: ctx.rpcConfig.rpcLimitations,
    maxTotalRetryMs: 60 * 1000,
    noLockIfNoLimit: true,
  });
  const getBlock = (blockNumber: number | "latest", msg: string): Promise<BlockRef> =>
    callLockProtectedRpc(() => provider.getBlock(blockNumber), callOptions(msg)).then((block) => ({
      number: block.number,
      timestamp: block.timestamp,
    }));

  // 1. latest block
  const latest = await getBlock("latest", "fallback creation: latest block");

  // 2. a block sampled some blocks back, to measure the current average block time of the chain
  const sampleNumber = Math.max(0, latest.number - BLOCK_TIME_SAMPLE_BLOCKS);
  const sample = await getBlock(sampleNumber, "fallback creation: sample block");
  const blocksElapsed = latest.number - sample.number;
  const secondsElapsed = latest.timestamp - sample.timestamp;
  if (blocksElapsed === 0) {
    // only happens on a chain with a single block
    return { blockNumber: latest.number, datetime: new Date(latest.timestamp * 1000), createdAt };
  }
  if (secondsElapsed <= 0) {
    throw new Error(`Cannot measure block time: ${secondsElapsed}s for ${blocksElapsed} blocks`);
  }

  // 3. the block for the target time, estimated then corrected with probes
  // the probes fetch real blocks, so the returned number and datetime always match
  const model: BlockTimeModel = {
    secondsPerBlock: secondsElapsed / blocksElapsed,
    change: BLOCK_TIME_CHANGES[ctx.chain],
  };
  const block = await findBlockNearTimestamp({
    latest,
    targetTimestamp,
    model,
    getBlock: (blockNumber) => getBlock(blockNumber, "fallback creation: probe block"),
    maxCorrections: CREATION_BLOCK_MAX_CORRECTIONS,
    toleranceSeconds: CREATION_BLOCK_TOLERANCE_SECONDS,
  });

  logger.info({
    msg: "Using beefy api createdAt minus buffer as contract creation",
    data: { chain: ctx.chain, contractAddress, createdAt, blockNumber: block.number, secondsPerBlock: model.secondsPerBlock },
  });
  return { blockNumber: block.number, datetime: new Date(block.timestamp * 1000), createdAt };
}

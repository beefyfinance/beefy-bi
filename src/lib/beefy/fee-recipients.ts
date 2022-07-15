import { Chain } from "../../types/chain";
import { DATA_DIRECTORY, LOG_LEVEL } from "../../utils/config";
import { LocalFileStore } from "../../utils/local-file-store";
import _BeefyStrategyFeeRecipientAbi from "../../../data/interfaces/beefy/BeefyStrategy/BeefyStrategyFeeRecipients.json";
import * as path from "path";
import { logger } from "../../utils/logger";
import { normalizeAddress } from "../../utils/ethers";
import { BeefyFeeRecipientInfo } from "../../types/beefy";
import { callLockProtectedRpc } from "../shared-resources/shared-rpc";
import { ethers } from "ethers";
import { JsonAbi } from "../../types/abi";
import { contractCreationStore } from "../json-store/contract-first-last-blocks";
import { isEqual, isNumber } from "lodash";
import { blockSamplesStore } from "../csv-store/csv-block-samples";

const BeefyStrategyFeeRecipientAbi = _BeefyStrategyFeeRecipientAbi as any as JsonAbi;

export const feeRecipientsStore = new LocalFileStore({
  loggerScope: "FEE.REC.STORE",
  doFetch: async (chain: Chain, contractAddress: string): Promise<BeefyFeeRecipientInfo> => {
    return fetchBeefyStrategyFeeRecipients(chain, contractAddress);
  },
  format: "json",
  getLocalPath: (chain: Chain, contractAddress: string) =>
    path.join(DATA_DIRECTORY, "chain", chain, "contracts", normalizeAddress(contractAddress), "fee_recipients.json"),
  getResourceId: (chain: Chain, contractAddress: string) => `${chain}:${contractAddress}:fee_recipients`,
  datefields: [],
  ttl_ms: 1000 * 60 * 60 * 24 * 7, // should expire in case we update fee recipients, set to 1 week
  retryOnFetchError: false, // we already have a retry on the work function
});

async function fetchBeefyStrategyFeeRecipients(chain: Chain, contractAddress: string): Promise<BeefyFeeRecipientInfo> {
  logger.debug(`Fetching fee recipients for ${chain}:${contractAddress}`);

  // find the data at the contract creation block
  // find the data now
  // if they are the same, we're good
  // if we can't get the data at creation time, let's say we are ok and return the latest data
  // if they differ, for each month we want to check if the fee recipient changed

  const creationInfos = await contractCreationStore.fetchData(chain, contractAddress);
  let creationFeeRecipient: BeefyFeeRecipientInfoAtBlock | null = null;
  if (creationInfos) {
    try {
      creationFeeRecipient = await getFeeRecipientsAtBlock(chain, contractAddress, creationInfos.blockNumber + 1);
    } catch (e) {
      logger.warn(
        `[FEE.REC.STORE] Error getting fee recipients at creation block (${creationInfos.blockNumber} + 1) for ${chain}:${contractAddress}: ${e}`
      );
      if (LOG_LEVEL === "trace") {
        console.log(e);
      }
    }
  } else {
    logger.warn(`[FEE.REC.STORE] Could not find creation infos for ${chain}:${contractAddress}`);
  }

  const currentFeeRecipient = await getFeeRecipientsAtBlock(chain, contractAddress, "latest");
  if (!creationFeeRecipient) {
    logger.info(`[FEE.REC.STORE] No creation infos for ${chain}:${contractAddress}, only using current data`);
    return {
      chain,
      contractAddress,
      recipientsAtBlock: [currentFeeRecipient],
    };
  }

  if (
    creationFeeRecipient.beefyFeeRecipient === currentFeeRecipient.beefyFeeRecipient &&
    creationFeeRecipient.strategist === currentFeeRecipient.strategist
  ) {
    logger.debug(`[FEE.REC.STORE] Fee recipients are the same for ${chain}:${contractAddress}, returning current data`);
    return {
      chain,
      contractAddress,
      recipientsAtBlock: [currentFeeRecipient],
    };
  }

  // now we are in the case where they differ, so the fee recipients was updated in the lifetime of this strategy
  // we go through the list of blocks and check the fee recipient each month only to get the list of fee recipients
  const blockList = blockSamplesStore.getReadIteratorFrom(
    (block) => block.blockNumber > creationInfos.blockNumber,
    chain,
    "4hour"
  );
  let currentMonth = creationInfos.datetime.toISOString().substring(0, 7);
  const allFeeRecipients: BeefyFeeRecipientInfoAtBlock[] = [creationFeeRecipient];
  for await (const block of blockList) {
    const blockMonth = block.datetime.toISOString().substring(0, 7);
    if (blockMonth === currentMonth) {
      continue;
    }
    currentMonth = blockMonth;

    logger.debug(
      `[FEE.REC.STORE] Checking fee recipients for ${chain}:${contractAddress} at block ${
        block.blockNumber
      }:${block.datetime.toISOString()}`
    );

    try {
      const feeRecipients = await getFeeRecipientsAtBlock(chain, contractAddress, block.blockNumber);
      allFeeRecipients.push(feeRecipients);
    } catch (e) {
      logger.warn(
        `[FEE.REC.STORE] Error getting fee recipients at block ${block.blockNumber}, skipping this block: ${e}`
      );
      if (LOG_LEVEL === "trace") {
        console.log(e);
      }
    }
  }
  allFeeRecipients.push(currentFeeRecipient);

  const uniqueFeeRecipients: BeefyFeeRecipientInfoAtBlock[] = [creationFeeRecipient];
  for (let i = 1; i < allFeeRecipients.length; i++) {
    if (isEqual({ ...allFeeRecipients[i], blockTag: null }, { ...allFeeRecipients[i - 1], blockTag: null })) {
      continue;
    }
    uniqueFeeRecipients.push(allFeeRecipients[i]);
  }

  // now we have all the fee recipients
  return {
    chain,
    contractAddress,
    recipientsAtBlock: uniqueFeeRecipients,
  };
}

interface BeefyFeeRecipientInfoAtBlock {
  chain: Chain;
  contractAddress: string;
  blockTag: number | "latest";
  beefyFeeRecipient: string | null;
  strategist: string;
}

async function getFeeRecipientsAtBlock(
  chain: Chain,
  contractAddress: string,
  blockTag: number | "latest"
): Promise<BeefyFeeRecipientInfoAtBlock> {
  logger.debug(`[FEE.REC.STORE] Getting fee recipients for ${chain}:${contractAddress} at block ${blockTag}`);
  const hexTag = isNumber(blockTag) ? ethers.utils.hexValue(blockTag) : blockTag;
  // first, get a lock on the rpc
  return callLockProtectedRpc(chain, async (provider) => {
    // instanciate the strategy contract
    const contract = new ethers.Contract(contractAddress, BeefyStrategyFeeRecipientAbi, provider);

    // get the fee recipients
    const strategist = await contract.strategist({ blockTag: hexTag });

    // beefy maxi strat don't have this field
    let beefyFeeRecipient = null;
    try {
      beefyFeeRecipient = await contract.beefyFeeRecipient({ blockTag: hexTag });
    } catch (e) {
      logger.debug(`[FEE.REC.STORE] ${chain}:${contractAddress} doesn't have a fee recipient at block ${blockTag}`);
      if (LOG_LEVEL === "trace") {
        console.log(e);
      }
    }

    return {
      chain,
      contractAddress,
      blockTag,
      beefyFeeRecipient,
      strategist,
    };
  });
}

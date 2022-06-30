import { logger } from "../utils/logger";
import { Chain } from "../types/chain";
import _BeefyStrategyFeeRecipientAbi from "../../data/interfaces/beefy/BeefyStrategy/BeefyStrategyFeeRecipients.json";
import * as ethers from "ethers";
import { callLockProtectedExplorerUrl } from "./shared-resources/shared-explorer";
import * as lodash from "lodash";
import { RPC_URLS } from "../utils/config";
import axios from "axios";
import { callLockProtectedRpc } from "./shared-resources/shared-rpc";
import { JsonAbi } from "../types/abi";

const BeefyStrategyFeeRecipientAbi =
  _BeefyStrategyFeeRecipientAbi as any as JsonAbi;

export interface BeefyFeeRecipientInfo {
  chain: Chain;
  contractAddress: string;
  strategist: string;
  beefyFeeRecipient: string;
}

export async function fetchBeefyStrategyFeeRecipients(
  chain: Chain,
  contractAddress: string
): Promise<BeefyFeeRecipientInfo> {
  logger.debug(`Fetching fee recipients for ${chain}:${contractAddress}`);

  // first, get a lock on the rpc
  return callLockProtectedRpc(chain, async (provider) => {
    // instanciate the strategy contract
    const contract = new ethers.Contract(
      contractAddress,
      BeefyStrategyFeeRecipientAbi,
      provider
    );

    // get the fee recipients
    const beefyFeeRecipient = await contract.beefyFeeRecipient();
    const strategist = await contract.strategist();

    return {
      chain,
      contractAddress,
      beefyFeeRecipient,
      strategist,
    };
  });
}

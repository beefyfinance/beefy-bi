import { Chain } from "../../types/chain";
import { DATA_DIRECTORY } from "../../utils/config";
import { LocalFileStore } from "../../utils/local-file-store";
import _BeefyStrategyFeeRecipientAbi from "../../../data/interfaces/beefy/BeefyStrategy/BeefyStrategyFeeRecipients.json";
import * as path from "path";
import { logger } from "../../utils/logger";
import { normalizeAddress } from "../../utils/ethers";
import { BeefyFeeRecipientInfo } from "../../types/beefy";
import { callLockProtectedRpc } from "../shared-resources/shared-rpc";
import { ethers } from "ethers";
import { JsonAbi } from "../../types/abi";

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
  ttl_ms: null, // should never expire
  retryOnFetchError: false, // we already have a retry on the work function
});

async function fetchBeefyStrategyFeeRecipients(chain: Chain, contractAddress: string): Promise<BeefyFeeRecipientInfo> {
  logger.debug(`Fetching fee recipients for ${chain}:${contractAddress}`);

  // first, get a lock on the rpc
  return callLockProtectedRpc(chain, async (provider) => {
    // instanciate the strategy contract
    const contract = new ethers.Contract(contractAddress, BeefyStrategyFeeRecipientAbi, provider);

    // get the fee recipients
    const strategist = await contract.strategist();

    // beefy maxi strat don't have this field
    let beefyFeeRecipient = null;
    try {
      await contract.beefyFeeRecipient();
    } catch (e) {}

    return {
      chain,
      contractAddress,
      beefyFeeRecipient,
      strategist,
    };
  });
}

import { RPC_URLS } from "./config";
import { Chain } from "../types/chain";
import * as ethers from "ethers";
import { cacheAsyncResultInRedis } from "./cache";
import { callLockProtectedRpc } from "../lib/shared-resources/shared-rpc";
import * as lodash from "lodash";
import axios from "axios";
import { isNumber } from "lodash";
import { logger } from "./logger";

export interface BlockDateInfos {
  blockNumber: number;
  datetime: Date;
}

export async function fetchBlockData(
  chain: Chain,
  blockNumber: ethers.ethers.providers.BlockTag
): Promise<BlockDateInfos> {
  logger.debug(`[BLOCKS] Fetching block ${chain}:${blockNumber}`);
  return callLockProtectedRpc(chain, async (provider) => {
    // for some reason ethers don't understand celo's response
    if (chain === "celo") {
      // documentation: https://www.quicknode.com/docs/ethereum/eth_getBlockByNumber
      let blockParam = isNumber(blockNumber)
        ? ethers.utils.hexlify(blockNumber)
        : blockNumber;
      // FIXES: invalid argument 0: hex number with leading zero digits
      if (blockParam.startsWith("0x0")) {
        blockParam = blockParam.replace(/^0x0+/, "0x");
      }
      const res = await axios.post<{
        result: { timestamp: string; number: string };
      }>(provider.connection.url, {
        method: "eth_getBlockByNumber",
        params: [blockParam, false],
        id: 1,
        jsonrpc: "2.0",
      });
      const blockRes = res.data.result;
      if (!blockRes || blockRes?.number === undefined) {
        throw new Error(
          `Invalid block result for celo ${chain}:${blockNumber} ${JSON.stringify(
            res.data
          )}`
        );
      }
      const blocknum = ethers.BigNumber.from(blockRes.number).toNumber();

      const datetime = new Date(
        ethers.BigNumber.from(blockRes.timestamp).toNumber() * 1000
      );
      return {
        blockNumber: blocknum,
        datetime,
      };
    } else {
      const block = await provider.getBlock(blockNumber);

      return {
        blockNumber: block.number,
        datetime: new Date(block.timestamp * 1000),
      };
    }
  });
}

export const getRedisCachedBlockDate = cacheAsyncResultInRedis(fetchBlockData, {
  getKey: (chain, blockNumber) => `${chain}:${blockNumber}`,
  dateFields: ["datetime"],
});

export function normalizeAddress(address: string) {
  return ethers.utils.getAddress(address);
}

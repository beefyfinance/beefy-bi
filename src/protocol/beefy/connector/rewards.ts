import Decimal from "decimal.js";
import { ethers } from "ethers";
import { Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";
import { ErrorEmitter, ImportCtx } from "../../common/types/import-context";
import { batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "beefy", component: "ppfs" });

interface BeefyPendingRewardsParams {
  blockNumber: number;
  contractAddress: string;
  tokenDecimals: number;
  ownerAddress: string;
}

export function fetchBeefyPendingRewards$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends BeefyPendingRewardsParams>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getPendingRewardsParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, ppfs: Decimal) => TRes;
}) {
  const logInfos = { msg: "Fetching Beefy pending rewards", data: { chain: options.ctx.chain } };
  return batchRpcCalls$({
    ctx: options.ctx,
    emitError: options.emitError,
    logInfos,
    rpcCallsPerInputObj: {
      eth_call: 1,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 0,
      eth_getTransactionReceipt: 0,
    },
    getQuery: options.getPendingRewardsParams,
    processBatch: (provider, params) => fetchBeefyGovOrBoostPendingRewards(provider, options.ctx.chain, params),
    formatOutput: options.formatOutput,
  });
}

// we just need the earned function abi
const BoostAndGovAbi = [
  {
    constant: true,
    inputs: [
      {
        name: "account",
        type: "address",
      },
    ],
    name: "earned",
    outputs: [
      {
        name: "",
        type: "uint256",
      },
    ],
    payable: false,
    stateMutability: "view",
    type: "function",
  },
];

async function fetchBeefyGovOrBoostPendingRewards<TParams extends BeefyPendingRewardsParams>(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractCalls: TParams[],
): Promise<Map<TParams, Decimal>> {
  // short circuit if no calls
  if (contractCalls.length === 0) {
    return new Map();
  }

  logger.debug({
    msg: "Batch rewards",
    data: { chain, contractCalls: contractCalls.length },
  });

  type MapEntry = [TParams, Decimal];
  let pendingRewardsPromises: Promise<MapEntry>[] = [];

  // fetch all ppfs in one go, this will batch calls using jsonrpc batching
  for (const contractCall of contractCalls) {
    const contract = new ethers.Contract(contractCall.contractAddress, BoostAndGovAbi, provider);
    const rewardsPromise = contract.functions
      .earned(contractCall.ownerAddress, { blockTag: contractCall.blockNumber })
      .then(([rawPendingRewards]: [ethers.BigNumber]) => {
        // now apply decimals to the rewards
        let decimalPendingRewards = new Decimal(rawPendingRewards.toString()).div(new Decimal(10).pow(contractCall.tokenDecimals));
        return [contractCall, decimalPendingRewards] as MapEntry;
      });

    pendingRewardsPromises.push(rewardsPromise);
  }

  return new Map(await Promise.all(pendingRewardsPromises));
}

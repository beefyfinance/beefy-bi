import axios from "axios";
import Decimal from "decimal.js";
import { ethers } from "ethers";
import { get, zipWith } from "lodash";
import BeefyVaultV6Abi from "../../../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../../../utils/rpc/archive-node-needed";
import { ErrorEmitter, ImportCtx } from "../../common/types/import-context";
import { batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "beefy", component: "ppfs" });

interface BeefyPPFSCallParams {
  vaultDecimals: number;
  underlyingDecimals: number;
  vaultAddress: string;
  blockNumber: number;
}

export function fetchBeefyPPFS$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends BeefyPPFSCallParams>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getPPFSCallParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, ppfs: Decimal) => TRes;
}) {
  const logInfos = { msg: "Fetching Beefy PPFS", data: { chain: options.ctx.chain } };
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
    getQuery: options.getPPFSCallParams,
    processBatch: (provider, params) => fetchBeefyVaultPPFS(provider, options.ctx.chain, params),
    formatOutput: options.formatOutput,
  });
}

async function fetchBeefyVaultPPFS<TParams extends BeefyPPFSCallParams>(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractCalls: TParams[],
): Promise<Map<TParams, Decimal>> {
  // short circuit if no calls
  if (contractCalls.length === 0) {
    return new Map();
  }

  logger.debug({
    msg: "Batch fetching PPFS",
    data: { chain, contractCalls: contractCalls.length },
  });

  type PPFSEntry = [TParams, ethers.BigNumber];
  type MapEntry = [TParams, Decimal];
  let shareRatePromises: Promise<MapEntry>[] = [];

  // fetch all ppfs in one go, this will batch calls using jsonrpc batching
  for (const contractCall of contractCalls) {
    let rawPromise: Promise<[ethers.BigNumber]>;

    // it looks like ethers doesn't yet support harmony's special format or something
    // same for heco
    if (chain === "harmony" || chain === "heco") {
      rawPromise = fetchBeefyPPFSWithManualRPCCall(provider, chain, contractCall);
    } else {
      const contract = new ethers.Contract(contractCall.vaultAddress, BeefyVaultV6Abi, provider);
      rawPromise = contract.functions.getPricePerFullShare({ blockTag: contractCall.blockNumber });
    }

    const shareRatePromise = rawPromise
      .then(([ppfs]) => [contractCall, ppfs] as PPFSEntry)
      .catch((err) => {
        // sometimes, we get this error: "execution reverted: SafeMath: division by zero"
        // this means that the totalSupply is 0 so we set ppfs to zero
        if (get(err, "message", "").includes("SafeMath: division by zero")) {
          return [contractCall, ethers.BigNumber.from(0)] as PPFSEntry;
        } else {
          // otherwise, we pass the error through
          throw err;
        }
      })
      .then(([contractCall, ppfs]) => {
        const vaultShareRate = ppfsToVaultSharesRate(contractCall.vaultDecimals, contractCall.underlyingDecimals, ppfs);
        return [contractCall, vaultShareRate] as MapEntry;
      });

    shareRatePromises.push(shareRatePromise);
  }

  return new Map(await Promise.all(shareRatePromises));
}

// takes ppfs and compute the actual rate which can be directly multiplied by the vault balance
// this is derived from mooAmountToOracleAmount in beefy-v2 repo
function ppfsToVaultSharesRate(mooTokenDecimals: number, depositTokenDecimals: number, ppfs: ethers.BigNumber) {
  const mooTokenAmount = new Decimal("1.0");

  // go to chain representation
  const mooChainAmount = mooTokenAmount.mul(new Decimal(10).pow(mooTokenDecimals)).toDecimalPlaces(0);

  // convert to oracle amount in chain representation
  const oracleChainAmount = mooChainAmount.mul(new Decimal(ppfs.toString()));

  // go to math representation
  // but we can't return a number with more precision than the oracle precision
  const oracleAmount = oracleChainAmount.div(new Decimal(10).pow(mooTokenDecimals + depositTokenDecimals)).toDecimalPlaces(mooTokenDecimals);

  return oracleAmount;
}

/**
 * I don't know why this is needed but seems like ethers.js is not doing the right rpc call
 */
async function fetchBeefyPPFSWithManualRPCCall(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractCall: BeefyPPFSCallParams,
): Promise<[ethers.BigNumber]> {
  const url = provider.connection.url;

  // get the function call hash
  const abi = ["function getPricePerFullShare()"];
  const iface = new ethers.utils.Interface(abi);
  const callData = iface.encodeFunctionData("getPricePerFullShare");

  // somehow block tag has to be hex encoded for heco
  const batchParams = {
    method: "eth_call",
    params: [
      {
        from: null,
        to: contractCall.vaultAddress,
        data: callData,
      },
      ethers.utils.hexValue(contractCall.blockNumber),
    ],
    id: 1,
    jsonrpc: "2.0",
  };

  type ResItem =
    | {
        jsonrpc: "2.0";
        id: number;
        result: string;
      }
    | {
        jsonrpc: "2.0";
        id: number;
        error: string;
      };
  const result = await axios.post<ResItem>(url, batchParams);
  const res = result.data;

  if (isErrorDueToMissingDataFromNode(res)) {
    throw new ArchiveNodeNeededError(chain, res);
  } else if ("error" in res) {
    throw new Error("Error in fetching PPFS: " + JSON.stringify(res));
  }
  const ppfs = ethers.utils.defaultAbiCoder.decode(["uint256"], res.result) as any as [ethers.BigNumber];
  return ppfs;
}

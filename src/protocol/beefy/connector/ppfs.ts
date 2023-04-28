import axios from "axios";
import Decimal from "decimal.js";
import { ContractCallContext, Multicall } from "ethereum-multicall";
import { ethers } from "ethers";
import { get, isArray, uniq } from "lodash";
import { Chain } from "../../../types/chain";
import { BeefyVaultV6AbiInterface } from "../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../../../utils/rpc/archive-node-needed";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../../common/types/import-context";
import { RPCBatchCallResult, batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "beefy", component: "ppfs" });

export interface BeefyPPFSCallParams {
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
): Promise<RPCBatchCallResult<TParams, Decimal>> {
  // short circuit if no calls
  if (contractCalls.length === 0) {
    return { successes: new Map(), errors: new Map() };
  }

  logger.debug({
    msg: "Batch fetching PPFS",
    data: { chain, contractCalls: contractCalls.length },
  });

  // if all contract call have the same block number, we can use a multicall contract to spare some rpc calls
  const mcMap = MULTICALL3_ADDRESS_MAP[chain];
  const uniqBlockNumbers = uniq(contractCalls.map((c) => c.blockNumber));
  console.log(contractCalls.length, uniqBlockNumbers.length, mcMap);
  if (
    // all contract calls have the same block number
    contractCalls.length > 1 &&
    uniqBlockNumbers.length === 1 &&
    // this chain has the Multicall3 contract deployed
    mcMap &&
    // the block number we work on is after the mc contract creation
    mcMap.createdAtBlock < uniqBlockNumbers[0]
  ) {
    const blockNumber = uniqBlockNumbers[0];
    const ppfsAbi = {
      inputs: [],
      name: "getPricePerFullShare",
      outputs: [{ internalType: "uint256", name: "", type: "uint256" }],
      stateMutability: "view",
      type: "function",
    };
    const calls: ContractCallContext[] = [];
    for (const contractCall of contractCalls) {
      const reference = contractCall.vaultAddress.toLocaleLowerCase();
      calls.push({
        reference: reference,
        contractAddress: contractCall.vaultAddress,
        abi: [ppfsAbi] as any[],
        calls: [{ reference: reference, methodName: "getPricePerFullShare", methodParameters: [] }],
      });
    }

    const multicall = new Multicall({ ethersProvider: provider, tryAggregate: true, multicallCustomContractAddress: mcMap.multicallAddress });
    const mcRes = await multicall.call(calls, { blockNumber: ethers.utils.hexValue(blockNumber) });

    const successes: Map<TParams, Decimal> = new Map();
    const errors: Map<TParams, ErrorReport> = new Map();
    for (const contractCall of contractCalls) {
      const reference = contractCall.vaultAddress.toLocaleLowerCase();
      const value = mcRes.results[reference]?.callsReturnContext.find((c) => c.reference === reference);
      if (!value) {
        errors.set(contractCall, { infos: { msg: "Could not find reference in MultiCall result", data: { contractCall, reference, res: mcRes } } });
        continue;
      }

      // when vault is empty, we get an empty result array
      // but this happens when the RPC returns an error too so we can't process this
      // the sad story is that we don't get any details about the error
      if (value.returnValues.length === 0 || value.decoded === false || value.success === false) {
        errors.set(contractCall, {
          infos: { msg: "PPFS result coming from multicall could not be parsed", data: { contractCall, reference, res: mcRes } },
        });
        continue;
      }
      const rawPpfs: { type: "BigNumber"; hex: string } | ethers.BigNumber = extractRawPpfsFromFunctionResult(value.returnValues);
      const ppfs = "hex" in rawPpfs ? ethers.BigNumber.from(rawPpfs.hex) : rawPpfs instanceof ethers.BigNumber ? rawPpfs : null;
      if (!ppfs) {
        errors.set(contractCall, {
          infos: { msg: "Could not parse MultiCall result into a ppfs data point", data: { contractCall, reference, rawPpfs } },
        });
        continue;
      }
      const vaultShareRate = ppfsToVaultSharesRate(contractCall.vaultDecimals, contractCall.underlyingDecimals, ppfs);
      successes.set(contractCall, vaultShareRate);
    }

    return { successes, errors };
  }

  // fetch all ppfs in one go, this will batch calls using jsonrpc batching
  type PPFSEntry = [TParams, ethers.BigNumber];
  type MapEntry = [TParams, Decimal];
  type ErrEntry = [TParams, ErrorReport];
  type CallResult<T> = { type: "success"; data: T } | { type: "error"; data: ErrEntry };
  let shareRatePromises: Promise<CallResult<MapEntry>>[] = [];
  for (const contractCall of contractCalls) {
    let rawPromise: Promise<[ethers.BigNumber]>;

    const contract = new ethers.Contract(contractCall.vaultAddress, BeefyVaultV6AbiInterface, provider);
    rawPromise = contract.callStatic.getPricePerFullShare({ blockTag: contractCall.blockNumber });

    const shareRatePromise = rawPromise
      .then(
        (ppfs): CallResult<PPFSEntry> => ({
          type: "success",
          data: [contractCall, extractRawPpfsFromFunctionResult(ppfs)] as PPFSEntry,
        }),
      )
      // empty vaults WILL throw an error
      .catch((err): CallResult<PPFSEntry> => {
        if (isEmptyVaultPPFSError(err)) {
          return { type: "success", data: [contractCall, ethers.BigNumber.from(0)] as PPFSEntry };
        } else {
          return {
            type: "error",
            data: [contractCall, { error: err, infos: { msg: "Unrecoverable error while fetching ppfs", data: { contractCall } } }] as ErrEntry,
          };
        }
      })
      .then((res): CallResult<MapEntry> => {
        if (res.type === "success") {
          const vaultShareRate = ppfsToVaultSharesRate(contractCall.vaultDecimals, contractCall.underlyingDecimals, res.data[1]);
          return { type: "success", data: [contractCall, vaultShareRate] };
        } else if (res.type === "error") {
          return res;
        } else {
          throw new ProgrammerError({ msg: "Unmapped type", data: { res } });
        }
      });

    shareRatePromises.push(shareRatePromise);
  }

  const batchResults = await Promise.all(shareRatePromises);
  return {
    successes: new Map(batchResults.filter((r): r is { type: "success"; data: MapEntry } => r.type === "success").map((r) => r.data)),
    errors: new Map(batchResults.filter((r): r is { type: "error"; data: ErrEntry } => r.type === "error").map((r) => r.data)),
  };
}

// sometimes, we get this error: "execution reverted: SafeMath: division by zero"
// this means that the totalSupply is 0 so we set ppfs to zero
export function isEmptyVaultPPFSError(err: any) {
  if (!err) {
    return false;
  }
  const errorMessage = get(err, ["error", "message"]) || get(err, "message") || "";
  return errorMessage.includes("SafeMath: division by zero");
}

export function extractRawPpfsFromFunctionResult<T>(returnData: [T] | T): T {
  // some chains don't return an array (harmony, heco)
  return isArray(returnData) ? returnData[0] : returnData;
}

// takes ppfs and compute the actual rate which can be directly multiplied by the vault balance
// this is derived from mooAmountToOracleAmount in beefy-v2 repo
export function ppfsToVaultSharesRate(mooTokenDecimals: number, depositTokenDecimals: number, ppfs: ethers.BigNumber) {
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

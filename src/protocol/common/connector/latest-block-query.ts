import { ethers } from "ethers";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { samplingPeriodMs } from "../../../types/sampling";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, MS_PER_BLOCK_ESTIMATE } from "../../../utils/config";

export function addLatestBlockQuery$<TObj, TRes>(options: {
  chain: Chain;
  provider: ethers.providers.JsonRpcProvider;
  getLastImportedBlock: (chain: Chain) => number | null;
  formatOutput: (
    obj: TObj,
    latestBlockQuery: { latestBlockNumber: number; fromBlock: number; toBlock: number },
  ) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // batch queries
    Rx.bufferCount(200),

    // go get the latest block number for this chain
    Rx.mergeMap(async (objs) => {
      const latestBlockNumber = await options.provider.getBlockNumber();
      return { objs, latestBlockNumber };
    }),

    // compute the block range we want to query
    Rx.map((objGroup) => {
      // fetch the last hour of data
      const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[options.chain];
      const period = samplingPeriodMs["1hour"];
      const periodInBlockCountEstimate = Math.floor(period / MS_PER_BLOCK_ESTIMATE[options.chain]);

      const lastImportedBlockNumber = options.getLastImportedBlock(options.chain);
      const diffBetweenLastImported = lastImportedBlockNumber
        ? objGroup.latestBlockNumber - (lastImportedBlockNumber + 1)
        : Infinity;

      const blockCountToFetch = Math.min(maxBlocksPerQuery, periodInBlockCountEstimate, diffBetweenLastImported);
      const fromBlock = objGroup.latestBlockNumber - blockCountToFetch;
      const toBlock = objGroup.latestBlockNumber;

      // also wait some time to avoid errors like "cannot query with height in the future; please provide a valid height: invalid height"
      // where the RPC don't know about the block number he just gave us
      const waitForBlockPropagation = 5;
      return {
        objs: objGroup.objs,
        latestBlocksQuery: {
          fromBlock: fromBlock - waitForBlockPropagation,
          toBlock: toBlock - waitForBlockPropagation,
          latestBlockNumber: objGroup.latestBlockNumber,
        },
      };
    }),

    // flatten and format the group
    Rx.mergeMap((objGroup) =>
      Rx.from(objGroup.objs.map((obj) => options.formatOutput(obj, objGroup.latestBlocksQuery))),
    ),
  );
}

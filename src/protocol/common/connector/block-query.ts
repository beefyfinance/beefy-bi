import { ethers } from "ethers";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { samplingPeriodMs } from "../../../types/sampling";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, MS_PER_BLOCK_ESTIMATE } from "../../../utils/config";
import { DbImportStatus, fetchImportStatus$, upsertImportStatus$ } from "../loader/import-status";
import { fetchContractCreationInfos$ } from "./contract-creation";

interface BlockQuery {
  fromBlock: number;
  toBlock: number;
}

/**
 * Generate a query based on the block
 * used to get last data for the given chain
 */
export function addLatestBlockQuery$<TObj, TRes>(options: {
  chain: Chain;
  provider: ethers.providers.JsonRpcProvider;
  getLastImportedBlock: (chain: Chain) => number | null;
  formatOutput: (obj: TObj, latestBlockQuery: BlockQuery) => TRes;
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
        },
      };
    }),

    // flatten and format the group
    Rx.mergeMap((objGroup) =>
      Rx.from(objGroup.objs.map((obj) => options.formatOutput(obj, objGroup.latestBlocksQuery))),
    ),
  );
}

/*
export function addHistoricalBlockQuery$<TObj, TRes>(options: {
  client: PoolClient;
  chain: Chain;
  provider: ethers.providers.JsonRpcProvider;
  getProductId: (obj: TObj) => number;
  getContractAddress: (obj: TObj) => string;
  formatOutput: (obj: TObj, historicalBlockQuery: BlockQuery) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // find the import status for these objects
    fetchImportStatus$({
      client: options.client,
      getProductId: options.getProductId,
      formatOutput: (obj, importStatus) => ({ obj, importStatus }),
    }),

    // extract those without an import status
    Rx.groupBy((item) => item.importStatus === null),
    Rx.map((isGroup$) => {
      // passthrough if we already have an import status
      if (isGroup$.key) {
        return isGroup$ as Rx.GroupedObservable<boolean, { obj: TObj; importStatus: DbImportStatus }>;
      }

      // then for those whe can't find an import status
      return isGroup$.pipe(
        // find the contract creation block
        fetchContractCreationInfos$({
          provider: options.provider,
          getCallParams: (item) => ({
            chain: options.chain,
            contractAddress: options.getContractAddress(item.obj),
          }),
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // create the import status
        upsertImportStatus$({
          client: options.client,
          getImportStatusData: (item) => ({
            productId: options.getProductId(item.obj),
            importData: {
              type: "beefy",
              data: {
                contractCreatedAtBlock: item.contractCreationInfo.blockNumber,
                importedBlockRange: {
                  from: item.contractCreationInfo.blockNumber,
                  to: item.contractCreationInfo.blockNumber,
                },
                blockRangesToRetry: [],
              },
            },
          }),
          formatOutput: (item, importStatus) => ({ ...item, importStatus }),
        }),
      );
    }),
    // now all objects have an import status (and a contract creation block)
    Rx.mergeAll(),

    // we can now create the historical block query
    Rx.map((item) => {
      const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[options.chain];
      const fromBlock = item.importStatus.importData.data.importedBlockRange.to - 1;
      const toBlock = fromBlock + maxBlocksPerQuery;
      return {
        ...item,
        historicalBlockQuery: {
          fromBlock,
          toBlock,
        },
      };
    }),

    // unwrap the object and format the output
    Rx.map((item) => options.formatOutput(item.obj, item.historicalBlockQuery)),
  );
}
*/

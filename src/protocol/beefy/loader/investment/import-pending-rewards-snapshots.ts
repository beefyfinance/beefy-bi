import Decimal from "decimal.js";
import { groupBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { db_query } from "../../../../utils/db";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { generateSnapshotQueriesFromEntryAndExits$ } from "../../../common/connector/import-queries";
import { fetchERC20TokenBalance$ } from "../../../common/connector/owner-balance";
import { DbPendingRewardsImportState, fetchImportState$ } from "../../../common/loader/import-state";
import { upsertInvestmentRewards$ } from "../../../common/loader/investment";
import { DbInvestor } from "../../../common/loader/investor";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct } from "../../../common/loader/product";
import { ErrorEmitter, ImportCtx } from "../../../common/types/import-context";
import { ImportRangeQuery, ImportRangeResult } from "../../../common/types/import-query";
import { dbBatchCall$ } from "../../../common/utils/db-batch";
import { isProductDashboardEOL } from "../../../common/utils/eol";
import { createHistoricalImportRunner } from "../../../common/utils/historical-recent-pipeline";
import { ChainRunnerConfig } from "../../../common/utils/rpc-chain-runner";
import { fetchBeefyPendingRewards$ } from "../../connector/rewards";
import { getProductContractAddress } from "../../utils/contract-accessors";
import { getInvestmentsImportStateKey, getPendingRewardImportStateKey } from "../../utils/import-state";
import { isBeefyBoost } from "../../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "pending-rewards-import" });

type PendingRewardSnapshotInput = {
  product: DbBeefyGovVaultProduct | DbBeefyBoostProduct;
  investor: DbInvestor;
};

export function createBeefyHistoricalPendingRewardsSnapshotsRunner(options: {
  chain: Chain;
  forceCurrentBlockNumber: number | null;
  runnerConfig: ChainRunnerConfig<PendingRewardSnapshotInput>;
}) {
  return createHistoricalImportRunner<PendingRewardSnapshotInput, number, DbPendingRewardsImportState>({
    runnerConfig: options.runnerConfig,
    logInfos: { msg: "Importing pending rewards snapshots", data: { chain: options.chain } },
    getImportStateKey: getPendingRewardImportStateKey,
    isLiveItem: (target) => !isProductDashboardEOL(target.product),
    createDefaultImportState$: (ctx) =>
      Rx.pipe(
        Rx.map((obj) => ({ obj })),

        // find the parent import state (to get the contract creation block)
        fetchImportState$({
          client: ctx.client,
          streamConfig: ctx.streamConfig,
          getImportStateKey: (item) => getInvestmentsImportStateKey(item.obj.product),
          formatOutput: (item, parentImportState) => ({ ...item, parentImportState }),
        }),

        excludeNullFields$("parentImportState"),

        Rx.map((item) => {
          if (item.parentImportState.importData.type !== "product:investment") {
            throw new ProgrammerError({ msg: "Unexpected import state type", data: { importState: item.parentImportState } });
          }
          return {
            obj: item.obj,
            importData: {
              type: "rewards:snapshots",
              chain: ctx.chain,
              productId: item.obj.product.productId,
              investorId: item.obj.investor.investorId,
              chainLatestBlockNumber: 0,
              contractCreatedAtBlock: item.parentImportState.importData.contractCreatedAtBlock,
              contractCreationDate: item.parentImportState.importData.contractCreationDate,
              ranges: {
                lastImportDate: new Date(),
                coveredRanges: [],
                toRetry: [],
              },
            },
          };
        }),
      ),
    generateQueries$: (ctx) =>
      Rx.pipe(
        // find all entries and exits of the investor to fill in the gaps
        dbBatchCall$({
          ctx,
          emitError: (item, report) => {
            logger.error(mergeLogsInfos({ msg: "Failed to fetch entries and exits", data: { item } }, report.infos));
            logger.error(report.error);
            throw new Error("Unable to fetch entries and exits");
          },
          logInfos: { msg: "Fetching pending rewards entry and exits", data: { chain: ctx.chain } },
          getData: (item) => ({ productId: item.target.product.productId, investorId: item.target.investor.investorId }),
          processBatch: async (objAndData) => {
            const res = await db_query<{
              datetime: string;
              block_number: number;
              product_id: number;
              investor_id: number;
              balance: string;
              balance_diff: string;
              is_entry: boolean;
            }>(
              ` 
                with query_scope as (
                    select product_id::integer, investor_id::integer
                    from (values %L) as t(product_id, investor_id)
                )
                select datetime, block_number, product_id, investor_id, balance, balance_diff, is_entry
                from (
                    -- entries
                    select datetime, block_number, product_id, investor_id, balance, balance_diff, true as is_entry
                    from investment_balance_ts
                    where balance = balance_diff
                    and (product_id, investor_id) in ( select product_id, investor_id from query_scope )
                    UNION ALL
                    -- exits;
                    select datetime, block_number, product_id, investor_id, balance, balance_diff, false as is_entry
                    from investment_balance_ts
                    where balance = 0
                    and (product_id, investor_id) in ( select product_id, investor_id from query_scope )
                ) as t
                order by product_id, investor_id, datetime asc, block_number asc
            `,
              [objAndData.map(({ data }) => [data.productId, data.investorId])],
              ctx.client,
            );

            const resultByData = groupBy(res, (row) => `${row.product_id}:${row.investor_id}`);

            return new Map(
              objAndData.map(({ data }) => {
                const rows = resultByData[`${data.productId}:${data.investorId}`];
                if (!rows) {
                  return [data, []];
                }
                return [data, rows];
              }),
            );
          },
          formatOutput: (item, entriesAndExits) => ({ ...item, entriesAndExits }),
        }),

        generateSnapshotQueriesFromEntryAndExits$({
          ctx,
          emitError: (item, report) => {
            logger.error(mergeLogsInfos({ msg: "Unable to generate snapshot queries", data: { item } }, report.infos));
            logger.error(report.error);
            throw new Error("Unable to generate snapshot queries");
          },
          timeStep: "4hour",
          forceCurrentBlockNumber: options.forceCurrentBlockNumber,
          getEntryAndExitEvents: (item) =>
            item.entriesAndExits.map((row) => ({
              block_number: row.block_number,
              is_entry: row.is_entry,
            })),
          getImportState: (item) => item.importState,
          formatOutput: (item, latestBlockNumber, queries) => ({ ...item, latestBlockNumber, queries }),
        }),

        // format appropriately
        Rx.concatMap((item) =>
          item.queries.map((query) => ({ target: item.target, range: query, latest: item.latestBlockNumber, importState: item.importState })),
        ),
      ),
    processImportQuery$: (ctx, emitError) => processPendingRewardSnapshotQuery$({ ctx, emitError }),
  });
}

function processPendingRewardSnapshotQuery$<
  TObj extends ImportRangeQuery<PendingRewardSnapshotInput, number> & { importState: DbPendingRewardsImportState },
  TErr extends ErrorEmitter<TObj>,
>(options: { ctx: ImportCtx; emitError: TErr }): Rx.OperatorFunction<TObj, ImportRangeResult<PendingRewardSnapshotInput, number>> {
  return Rx.pipe(
    // get the midpoint of the range
    Rx.map((item) => ({ ...item, rangeMidpoint: Math.floor((item.range.from + item.range.to) / 2) })),

    Rx.map((item) => ({
      ...item,
      rewardTokenDecimals: isBeefyBoost(item.target.product)
        ? item.target.product.productData.boost.reward_token_decimals
        : item.target.product.productData.vault.gov_vault_reward_token_decimals || 18,
      vaultTokenDecimals: isBeefyBoost(item.target.product)
        ? item.target.product.productData.boost.staked_token_decimals
        : item.target.product.productData.vault.token_decimals,
      contractAddress: getProductContractAddress(item.target.product),
    })),

    // fetch pending rewards at the midpoint
    fetchBeefyPendingRewards$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPendingRewardsParams: (item) => {
        return {
          blockNumber: item.rangeMidpoint,
          contractAddress: item.contractAddress,
          ownerAddress: item.target.investor.address,
          tokenDecimals: item.rewardTokenDecimals,
        };
      },
      formatOutput: (item, pendingRewards) => ({ ...item, pendingRewards }),
    }),

    fetchBlockDatetime$({
      ctx: options.ctx,
      emitError: options.emitError,
      getBlockNumber: (item) => item.rangeMidpoint,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),

    // we need the balance
    fetchERC20TokenBalance$({
      ctx: options.ctx,
      emitError: options.emitError,
      getQueryParams: (item) => ({
        blockNumber: item.rangeMidpoint,
        decimals: item.vaultTokenDecimals,
        contractAddress: item.contractAddress,
        ownerAddress: item.target.investor.address,
      }),
      formatOutput: (item, vaultBalance) => ({ ...item, vaultBalance }),
    }),

    // insert the snapshot
    upsertInvestmentRewards$({
      ctx: options.ctx,
      emitError: options.emitError,
      getInvestmentData: (item) => ({
        productId: item.target.product.productId,
        investorId: item.target.investor.investorId,
        blockNumber: item.rangeMidpoint,
        datetime: item.blockDatetime,
        balance: item.vaultBalance,
        pendingRewards: item.pendingRewards,
        pendingRewardsDiff: new Decimal(0),
        investmentData: {
          rewards: {
            query: { range: item.range, midPoint: item.rangeMidpoint, latest: item.latest },
            importDate: new Date().toISOString(),
          },
        },
      }),
      formatOutput: (item, result) => ({ ...item, result }),
    }),

    // transform to result
    Rx.map((item) => ({ ...item, success: true })),
  );
}

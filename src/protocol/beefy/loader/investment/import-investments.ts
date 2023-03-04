import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchContractCreationInfos$ } from "../../../common/connector/contract-creation";
import { addHistoricalBlockQuery$, addLatestBlockQuery$ } from "../../../common/connector/import-queries";
import { upsertBlock$ } from "../../../common/loader/blocks";
import { DbProductInvestmentImportState } from "../../../common/loader/import-state";
import { DbBeefyProduct } from "../../../common/loader/product";
import { isProductDashboardEOL } from "../../../common/utils/eol";
import { createHistoricalImportRunner, createRecentImportRunner } from "../../../common/utils/historical-recent-pipeline";
import { ChainRunnerConfig } from "../../../common/utils/rpc-chain-runner";
import { getProductContractAddress } from "../../utils/contract-accessors";
import { getInvestmentsImportStateKey } from "../../utils/import-state";
import { importProductBlockRange$ } from "./product-block-range";

const logger = rootLogger.child({ module: "beefy", component: "investment-import" });

export function createBeefyHistoricalInvestmentRunner(options: { chain: Chain; runnerConfig: ChainRunnerConfig<DbBeefyProduct> }) {
  return createHistoricalImportRunner<DbBeefyProduct, number, DbProductInvestmentImportState>({
    runnerConfig: options.runnerConfig,
    logInfos: { msg: "Importing historical beefy investments", data: { chain: options.chain } },
    getImportStateKey: getInvestmentsImportStateKey,
    isLiveItem: (p) => !isProductDashboardEOL(p),
    generateQueries$: (ctx, emitError) =>
      Rx.pipe(
        addHistoricalBlockQuery$({
          ctx,
          emitError,
          isLiveItem: (p) => !isProductDashboardEOL(p.target),
          getImport: (item) => item.importState,
          getFirstBlockNumber: (importState) => importState.importData.contractCreatedAtBlock,
          formatOutput: (item, latestBlockNumber, blockQueries) => blockQueries.map((range) => ({ ...item, range, latest: latestBlockNumber })),
        }),
        Rx.concatAll(),
      ),
    createDefaultImportState$: (ctx) =>
      Rx.pipe(
        // initialize the import state
        // find the contract creation block
        fetchContractCreationInfos$({
          ctx,
          getCallParams: (obj) => ({
            chain: ctx.chain,
            contractAddress: getProductContractAddress(obj),
          }),
          formatOutput: (obj, contractCreationInfo) => ({ obj, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        // add this block to our global block list
        upsertBlock$({
          ctx,
          emitError: (item, report) => {
            logger.error(mergeLogsInfos({ msg: "Failed to upsert block", data: { item } }, report.infos));
            logger.error(report.error);
            throw new Error("Failed to upsert block");
          },
          getBlockData: (item) => ({
            datetime: item.contractCreationInfo.datetime,
            chain: item.obj.chain,
            blockNumber: item.contractCreationInfo.blockNumber,
            blockData: {},
          }),
          formatOutput: (item, block) => ({ ...item, block }),
        }),

        Rx.map((item) => ({
          obj: item.obj,
          importData: {
            type: "product:investment",
            productId: item.obj.productId,
            chain: item.obj.chain,
            chainLatestBlockNumber: item.contractCreationInfo.blockNumber,
            contractCreatedAtBlock: item.contractCreationInfo.blockNumber,
            contractCreationDate: item.contractCreationInfo.datetime,
            ranges: {
              lastImportDate: new Date(),
              coveredRanges: [],
              toRetry: [],
            },
          },
        })),
      ),
    processImportQuery$: (ctx, emitError) =>
      importProductBlockRange$({ ctx, emitBoostError: emitError, emitGovVaultError: emitError, emitStdVaultError: emitError }),
  });
}

export function createBeefyRecentInvestmentRunner(options: { chain: Chain; runnerConfig: ChainRunnerConfig<DbBeefyProduct> }) {
  return createRecentImportRunner<DbBeefyProduct, number>({
    runnerConfig: options.runnerConfig,
    cacheKey: "beefy:product:investment:recent",
    logInfos: { msg: "Importing recent beefy investments", data: { chain: options.chain } },
    getImportStateKey: getInvestmentsImportStateKey,
    isLiveItem: (p) => !isProductDashboardEOL(p),
    generateQueries$: ({ ctx, emitError, lastImported, formatOutput }) =>
      addLatestBlockQuery$({
        ctx,
        emitError,
        getLastImportedBlock: () => lastImported,
        formatOutput: (item, latest, range) => formatOutput(item, latest, [range]),
      }),
    processImportQuery$: (ctx, emitError) =>
      importProductBlockRange$({ ctx, emitBoostError: emitError, emitGovVaultError: emitError, emitStdVaultError: emitError }),
  });
}

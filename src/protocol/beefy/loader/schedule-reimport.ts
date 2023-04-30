import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { DbClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { Range } from "../../../utils/range";
import { fetchImportState$, updateImportState$ } from "../../common/loader/import-state";
import { chainProductIds$, fetchProduct$ } from "../../common/loader/product";
import { ImportCtx } from "../../common/types/import-context";
import { NoRpcRunnerConfig, createChainRunner } from "../../common/utils/rpc-chain-runner";
import { getProductContractAddress } from "../utils/contract-accessors";
import { getInvestmentsImportStateKey } from "../utils/import-state";

const logger = rootLogger.child({ module: "beefy", component: "schedule-reimport" });

type ScheduleReimportInput = { chain: Chain; onlyAddress: string[] | null; reimport: Range<number> };

export function createScheduleReimportInvestmentsRunner(options: { client: DbClient; runnerConfig: NoRpcRunnerConfig<ScheduleReimportInput> }) {
  const emitError = (obj: any, report: any) => {
    logger.error({ msg: "Error emitted", data: { obj, report } });
    throw new Error("Error emitted");
  };
  return createChainRunner(options.runnerConfig, (ctx: ImportCtx) =>
    Rx.pipe(
      // fetch matching products
      Rx.pipe(
        chainProductIds$({
          ctx,
          emitError,
          getChain: (item) => item.chain,
          formatOutput: (item, productIds) => productIds.map((productId) => ({ ...item, productId })),
        }),
        Rx.concatAll(),
        fetchProduct$({
          ctx,
          emitError,
          getProductId: ({ productId }) => productId,
          formatOutput: (item, product) => ({ ...item, product }),
        }),
        Rx.filter(({ onlyAddress, product }) => onlyAddress === null || onlyAddress.includes(getProductContractAddress(product))),

        Rx.tap(({ product, chain }) =>
          logger.info({
            msg: "Scheduling reimport for product",
            data: { chain, productKey: product.productKey, productType: product.productData.type },
          }),
        ),
      ),

      // remove products where import state does not exists yet
      Rx.pipe(
        fetchImportState$({
          client: options.client,
          streamConfig: ctx.streamConfig,
          getImportStateKey: ({ product }) => getInvestmentsImportStateKey({ productId: product.productId }),
          formatOutput: (item, importState) => ({ ...item, importState }),
        }),

        Rx.filter(({ importState }) => importState !== null),
      ),

      // schedule the reimport of data
      Rx.pipe(
        Rx.map(({ chain, product, reimport }) => ({
          target: product,
          range: reimport,
          success: false,
        })),

        // schedule the product re-import
        updateImportState$({
          ctx,
          emitError,
          getImportStateKey: ({ target }) => getInvestmentsImportStateKey({ productId: target.productId }),
          getRange: ({ range }) => range,
          isSuccess: ({ success }) => success,
          formatOutput: (item, newImportState) => ({ ...item, newImportState }),
        }),
      ),

      Rx.tap(({ newImportState }) => console.dir(newImportState, { depth: null })),
    ),
  );
}

import Decimal from "decimal.js";
import { db_query } from "../../../utils/db";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

interface DbBeefyVaultStats {
  datetime: Date;
  blockNumber: number;
  productId: number;
  vaultTotalSupply: Decimal;
  shareToUnderlyingPrice: Decimal;
  stakedUnderlying: Decimal;
  underlyingTotalSupply: Decimal;
  underlyingCapturePercentage: number;
}

export function upsertBeefyVaultStats$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends DbBeefyVaultStats>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getVaultStatsData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, vaultStats: DbBeefyVaultStats) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getVaultStatsData,
    logInfos: { msg: "upsertBeefyVaultStats" },
    processBatch: async (objAndData) => {
      await db_query<{
        product_id: number;
        datetime: Date;
        block_number: number;
        vault_total_supply: string;
        share_to_underlying_price: string;
        staked_underlying: string;
        underlying_total_supply: string;
        underlying_capture_percentage: number;
      }>(
        `INSERT INTO beefy_vault_stats_ts (
            product_id,
            datetime,
            block_number,
            vault_total_supply,
            share_to_underlying_price,
            staked_underlying,
            underlying_total_supply,
            underlying_capture_percentage
        ) VALUES %L
            ON CONFLICT (product_id, block_number, datetime) 
            DO UPDATE SET 
                vault_total_supply = EXCLUDED.vault_total_supply,
                share_to_underlying_price = EXCLUDED.share_to_underlying_price,
                staked_underlying = EXCLUDED.staked_underlying,
                underlying_total_supply = EXCLUDED.underlying_total_supply,
                underlying_capture_percentage = EXCLUDED.underlying_capture_percentage
          RETURNING 
            product_id,
            datetime,
            block_number,
            vault_total_supply,
            share_to_underlying_price,
            staked_underlying,
            underlying_total_supply,
            underlying_capture_percentage
        `,
        [
          objAndData.map(({ data }) => [
            data.productId,
            data.datetime.toISOString(),
            data.blockNumber,
            data.vaultTotalSupply.toString(),
            data.shareToUnderlyingPrice.toString(),
            data.stakedUnderlying.toString(),
            data.underlyingTotalSupply.toString(),
            data.underlyingCapturePercentage,
          ]),
        ],
        options.ctx.client,
      );

      // update debug data
      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}

import { getInvestmentsImportStateKey } from "../../../protocol/beefy/utils/import-state";
import { isProductInvestmentImportState } from "../../../protocol/common/loader/import-state";
import { DbProduct } from "../../../protocol/common/loader/product";
import { DbClient, db_query } from "../../../utils/db";
import { BlockService } from "../block";
import { ImportStateService } from "../import-state";
import { ProductService } from "../product";

export class BeefyVaultService {
  constructor(private services: { db: DbClient; product: ProductService; importState: ImportStateService; block: BlockService }) {}

  public static investorBalanceSchemaComponents = {
    InvestorBalanceRow: {
      $id: "InvestorBalanceRow",
      type: "object",
      properties: {
        investor_address: { type: "string", description: "The investor address", example: "0x1234567890123456789012345678901234567890" },
        share_to_underlying_price: {
          type: "number",
          description: "Exchange rate between share token and underlying token. Almost PPFS but not quite.",
        },
        underlying_to_usd_price: {
          type: "number",
          description: "LP token price in USD",
        },
        share_token_balance: {
          type: "number",
          description: "The investor share token balance",
        },
        underlying_balance: {
          type: "number",
          description: "The investor underlying token balance",
        },
        usd_balance: {
          type: "number",
          description: "The investor USD balance",
        },
      },
      required: ["investor_address", "share_token_balance"],
    },
  };

  public static investorBalancesSchema = {
    description: "The investor balances",
    type: "array",
    items: { $ref: "InvestorBalanceRow" },
  };

  async getBalancesAtBlock(product: DbProduct, blockNumber: number) {
    const res = await this.services.product.getSingleProductPriceFeedIds(product.productId);
    if (res === null) {
      return [];
    }
    const { price_feed_1_id, price_feed_2_id } = res;

    // we need the contract creation date to let timescaledb know when to stop looking
    const importStateKey = getInvestmentsImportStateKey(product);
    const importState = await this.services.importState.getImportStateByKey(importStateKey);
    if (importState === null) {
      return [];
    }
    if (!isProductInvestmentImportState(importState)) {
      return [];
    }
    const contractCreationDate = importState.importData.contractCreationDate;

    // optimize further by using the next block date if we have it
    const nextBlock = await this.services.block.getFirstBlockAboveOrEqualToNumber(product.chain, blockNumber + 1);
    const filterDate = nextBlock ? nextBlock.datetime : new Date();

    return db_query<{
      investor_address: string;
      share_to_underlying_price: number | null;
      underlying_to_usd_price: number | null;
      share_token_balance: number;
      underlying_balance: number | null;
      usd_balance: number | null;
    }>(
      `
        with investor_last_balance as (
          select investor_id, last(balance, datetime) as last_balance
          from investment_balance_ts
          where 
              product_id = %L
              and datetime between %L and %L
              and block_number <= %L
          group by investor_id
          having last(balance, datetime) > 0
        ),
        price_1_at_last_balance as materialized (
          select 
              last(price, datetime) as last_price
          from price_ts
          where 
              price_feed_id = %L
              and datetime between %L and %L
              and block_number <= %L
        ),
        price_2_at_last_balance as materialized (
          select 
              last(price, datetime) as last_price
          from price_ts
          where 
              price_feed_id = %L
              and datetime between %L and %L
              and block_number <= %L
        )
        select 
          bytea_to_hexstr(i.address) as investor_address,
          p1.last_price as share_to_underlying_price,
          p2.last_price as underlying_to_usd_price,
          ts.last_balance as share_token_balance,
          ts.last_balance * p1.last_price as underlying_balance,
          p1.last_price * p2.last_price * ts.last_balance as usd_balance
        from investor_last_balance as ts,
          price_1_at_last_balance as p1,
          price_2_at_last_balance as p2,
          investor i
        where
          ts.investor_id = i.investor_id
        `,
      [
        product.productId,
        contractCreationDate.toISOString(),
        filterDate.toISOString(),
        blockNumber,
        price_feed_1_id,
        contractCreationDate.toISOString(),
        filterDate.toISOString(),
        blockNumber,
        price_feed_2_id,
        contractCreationDate.toISOString(),
        filterDate.toISOString(),
        blockNumber,
      ],
      this.services.db,
    );
  }
}

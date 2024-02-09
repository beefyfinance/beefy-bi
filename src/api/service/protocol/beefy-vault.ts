import { DbClient, db_query } from "../../../utils/db";
import { ProductService } from "../product";

export class BeefyVaultService {
  constructor(private services: { db: DbClient; product: ProductService }) {}

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

  async getBalancesAtBlock(productId: number, blockNumber: number) {
    const res = await this.services.product.getSingleProductPriceFeedIds(productId);
    if (res === null) {
      return [];
    }
    const { price_feed_1_id, price_feed_2_id } = res;

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
              and block_number <= %L
        ),
        price_2_at_last_balance as materialized (
          select 
              last(price, datetime) as last_price
          from price_ts
          where 
              price_feed_id = %L
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
      [productId, blockNumber, price_feed_1_id, blockNumber, price_feed_2_id, blockNumber],
      this.services.db,
    );
  }
}

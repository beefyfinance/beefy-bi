import { getInvestmentsImportStateKey } from "../../../protocol/beefy/utils/import-state";
import { isProductInvestmentImportState } from "../../../protocol/common/loader/import-state";
import { DbProduct } from "../../../protocol/common/loader/product";
import { DbClient, db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { BlockService } from "../block";
import { ImportStateService } from "../import-state";
import { ProductService } from "../product";
import { RpcService } from "../rpc";

const logger = rootLogger.child({ module: "api-service", component: "beefy-vault" });

export class BeefyVaultService {
  constructor(private services: { db: DbClient; product: ProductService; importState: ImportStateService; block: BlockService; rpc: RpcService }) {}

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

  async getBalancesAtBlock(product: DbProduct, blockNumber: number, blockDatetime: Date | null) {
    const res = await this.services.product.getSingleProductPriceFeedIds(product.productId);
    if (res === null) {
      throw new Error("Product not found");
    }
    const { price_feed_1_id, price_feed_2_id } = res;

    // we need the contract creation date to let timescaledb know when to stop looking
    const importStateKey = getInvestmentsImportStateKey(product);
    const importState = await this.services.importState.getImportStateByKey(importStateKey);
    if (importState === null) {
      throw new Error("Import state not found");
    }
    if (!isProductInvestmentImportState(importState)) {
      throw new Error("Import state is not for product investments");
    }
    logger.debug({ importState }, "import state");

    if (importState.importData.contractCreatedAtBlock > blockNumber) {
      throw new Error("This block is before the contract creation.");
    }

    if (importState.importData.chainLatestBlockNumber < blockNumber) {
      throw new Error("This block is not yet indexed. Please try again later. Last indexed block: " + importState.importData.chainLatestBlockNumber);
    }

    // ensure all the blocks are indexed
    importState.importData.ranges.toRetry.forEach((range) => {
      if (range.from < blockNumber) {
        throw new Error("Some previous block was not indexed. Please try again later.");
      }
    });

    const contractCreationDate = importState.importData.contractCreationDate;

    const block_datetime = blockDatetime ?? (await this.services.rpc.getBlockDatetime(product.chain, blockNumber));

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
              and datetime between %L and (%L::timestamptz + '1 day'::interval)
              and block_number <= %L
          group by investor_id
          having last(balance, datetime) > 0
        ),
        price_1_at_last_balance as materialized (
          select 
            price_avg as avg_price
          from price_ts_cagg_1h
          where 
              price_feed_id = %L
              and datetime = time_bucket('1h', %L::timestamptz)
        ),
        price_2_at_last_balance as materialized (
          select 
            price_avg as avg_price
          from price_ts_cagg_1h
          where 
              price_feed_id = %L
              and datetime = time_bucket('1h', %L::timestamptz)
        )
        select 
          bytea_to_hexstr(i.address) as investor_address,
          p1.avg_price as share_to_underlying_price,
          p2.avg_price as underlying_to_usd_price,
          ts.last_balance as share_token_balance,
          ts.last_balance * p1.avg_price as underlying_balance,
          p1.avg_price * p2.avg_price * ts.last_balance as usd_balance
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
        block_datetime.toISOString(),
        blockNumber,
        price_feed_1_id,
        block_datetime.toISOString(),
        price_feed_2_id,
        block_datetime.toISOString(),
      ],
      this.services.db,
    );
  }

  public static lineaBalanceSchemaComponents = {
    LineaInvestorBalanceRow: {
      $id: "LineaInvestorBalanceRow",
      type: "object",
      properties: {
        beefy_vault_id: { type: "string", description: "The beefy vault id", example: "lynex-wsteth-weth" },
        beefy_vault_address: { type: "string", description: "The beefy vault address", example: "0x1234567890123456789012345678901234567890" },
        want_address: { type: "string", description: "The underlying LP token address", example: "0x1234567890123456789012345678901234567890" },
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

  public static lineaBalancesSchema = {
    description: "The investor balances",
    type: "array",
    items: { $ref: "LineaInvestorBalanceRow" },
  };

  /**
   * specific for the linea chain
   */
  async getLineaAllInvestorBalancesAtBlock(blockNumber: number, blockDatetime: Date | null) {
    const chainLatestBlockNumber = await this.services.importState.getChainLatestIndexedBlock("linea", "product:investment");
    if (chainLatestBlockNumber === null) {
      throw new Error("Chain not found");
    }
    if (chainLatestBlockNumber < blockNumber) {
      throw new Error("This block is not yet indexed. Please try again later. Last indexed block: " + chainLatestBlockNumber);
    }

    // ensure all the blocks are indexed
    const hasAnythingToRetry = await this.services.importState.getChainHasAnythingToRetry("linea", "product:investment");
    if (hasAnythingToRetry) {
      throw new Error("Some previous block was not indexed. Please try again later.");
    }

    const block_datetime = blockDatetime ?? (await this.services.rpc.getBlockDatetime("linea", blockNumber));

    return db_query<{
      beefy_vault_id: string;
      beefy_vault_address: string;
      want_address: string;
      investor_address: string;
      share_to_underlying_price: number | null;
      underlying_to_usd_price: number | null;
      share_token_balance: number;
      underlying_balance: number | null;
      usd_balance: number | null;
    }>(
      `
        with product_scope as (
          select product_id, price_feed_1_id, price_feed_2_id
          from product
          where 
            chain = 'linea'
        ),
        investor_last_balance as (
          select investor_id, product_id, last(balance, datetime) as last_balance
          from investment_balance_ts
          where 
              product_id in (select product_id from product_scope)
              and datetime between '2023-12-01T00:00:00.000Z'::timestamptz and %L::timestamptz
              and block_number <= %L
          group by investor_id, product_id
          having last(balance, datetime) > 0
        ),
        price_1_at_last_balance as materialized (
          select 
            price_feed_id,
            price_avg as avg_price
          from price_ts_cagg_1h
          where 
              price_feed_id in (
                select price_feed_1_id from product_scope
              )
              and datetime = time_bucket('1h', %L::timestamptz)
        ),
        price_2_at_last_balance as materialized (
          select 
            price_feed_id,
            price_avg as avg_price
          from price_ts_cagg_1h
          where 
              price_feed_id in (
                select price_feed_2_id from product_scope
              )
              and datetime = time_bucket('1h', %L::timestamptz)
        )
        select 
          p.product_data->'vault'->>'id' as beefy_vault_id,
          p.product_data->'vault'->>'contract_address' as beefy_vault_address,
          p.product_data->'vault'->>'want_address' as want_address,
          bytea_to_hexstr(i.address) as investor_address,
          p1.avg_price as share_to_underlying_price,
          p2.avg_price as underlying_to_usd_price,
          ts.last_balance as share_token_balance,
          ts.last_balance * p1.avg_price as underlying_balance,
          p1.avg_price * p2.avg_price * ts.last_balance as usd_balance
        from investor_last_balance as ts
        join product_scope as ps on ts.product_id = ps.product_id
        join price_1_at_last_balance as p1 on ps.price_feed_1_id = p1.price_feed_id
        join price_2_at_last_balance as p2 on ps.price_feed_2_id = p2.price_feed_id
        join investor as i on ts.investor_id = i.investor_id
        join product as p on ts.product_id = p.product_id
        `,
      [block_datetime.toISOString(), blockNumber, block_datetime.toISOString(), block_datetime.toISOString()],
      this.services.db,
    );
  }
}

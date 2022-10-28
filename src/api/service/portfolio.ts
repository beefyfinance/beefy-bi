import Decimal from "decimal.js";
import { PoolClient } from "pg";
import { boolean, string } from "yargs";
import { allChainIds, Chain } from "../../types/chain";
import { db_query } from "../../utils/db";
import { AsyncCache } from "./cache";
import { ProductService } from "./product";

export class PortfolioService {
  constructor(private services: { db: PoolClient; cache: AsyncCache; product: ProductService }) {}

  getInvestedProducts(investorId: number, chains: Chain[]) {
    return db_query<{
      product_id: number;
      last_balance: string;
    }>(
      `
      with last_balances as (
        select p.product_id, last(balance::numeric, b.datetime) as last_balance
        from investment_balance_ts b
        join product p on p.product_id = b.product_id
        where investor_id = %L and chain in (%L)
        group by 1
      )
      select *
      from last_balances
      where last_balance is not null
        and last_balance > 0
    `,
      [investorId, chains],
      this.services.db,
    );
  }

  async getInvestorPortfolioValue(investorId: number) {
    const cacheKey = `api:portfolio-service:current-value:${investorId}}`;
    const ttl = 1000 * 60 * 5; // 5 min
    return this.services.cache.wrap(cacheKey, ttl, async () => {
      const investedProducts = await this.getInvestedProducts(investorId, allChainIds);
      const productIds = investedProducts.map((p) => p.product_id);
      const priceFeedIDs = await this.services.product.getPriceFeedIds(productIds);
      const priceFeed1Ids = priceFeedIDs.map((pfs) => pfs.price_feed_1_id);
      const priceFeed2Ids = priceFeedIDs.map((pfs) => pfs.price_feed_2_id);

      return db_query<{
        product_id: number;
        product_key: string;
        chain: Chain;
        is_eol: boolean;
        share_to_underlying_price: string;
        underlying_to_usd_price: string;
        share_balance: string;
        underlying_balance: string;
        usd_balance: string;
      }>(
        `
          with share_balance as (
            SELECT
              b.product_id,
              last(b.balance::numeric, b.datetime) as share_balance,
              last(b.datetime, b.datetime) as share_last_time
            FROM
              investment_balance_ts b
            WHERE
              b.investor_id = %L
              and b.product_id in (select unnest(ARRAY[%L]::integer[]))
            group by 1
          ),
          price_1 as (
            SELECT
              p1.price_feed_id,
              last(p1.price::numeric, p1.datetime) as price,
              last(p1.datetime, p1.datetime) as last_time
            FROM
              price_ts p1
            WHERE
              p1.price_feed_id in (select unnest(ARRAY[%L]::integer[]))
            group by 1
          ),
          price_2 as (
            SELECT
              p2.price_feed_id,
              last(p2.price::numeric, p2.datetime) as price,
              last(p2.datetime, p2.datetime) as last_time
            FROM
              price_ts p2
            WHERE
              p2.price_feed_id in (select unnest(ARRAY[%L]::integer[]))
            group by 1
          )
          select 
            p.product_id,
            p.product_key,
            p.chain,
            coalesce(p.product_data->'vault'->>'eol', p.product_data->'boost'->>'eol')::text = 'true' as is_eol,
            p1.price as share_to_underlying_price, 
            p2.price as underlying_to_usd_price,
            b.share_balance::NUMERIC(100, 24), 
            (b.share_balance * p1.price)::NUMERIC(100, 24) as underlying_balance,
            (b.share_balance * p1.price * p2.price)::NUMERIC(100, 24) as usd_balance
          from share_balance b
            left join product p on b.product_id = p.product_id
            left join price_1 p1 on p.price_feed_1_id = p1.price_feed_id
            left join price_2 p2 on p.price_feed_2_id = p2.price_feed_id
          order by 1
        `,
        [investorId, productIds, priceFeed1Ids, priceFeed2Ids],
        this.services.db,
      );
    });
  }

  async getInvestorTimeline(investorId: number) {
    const cacheKey = `api:portfolio-service:timeline:${investorId}}`;
    const ttl = 1000 * 60 * 5; // 5 min
    return this.services.cache.wrap(cacheKey, ttl, async () => {
      const investedProducts = await this.getInvestedProducts(investorId, allChainIds);
      const productIds = investedProducts.map((p) => p.product_id);

      return db_query<{
        datetime: Date;
        product_key: string;
        chain: Chain;
        is_eol: boolean;
        share_to_underlying_price: string;
        underlying_to_usd_price: string;
        share_balance: string;
        underlying_balance: string;
        usd_balance: string;
        share_diff: string;
        underlying_diff: string;
        usd_diff: string;
      }>(
        `
          with investment_diff_raw as (
            select b.datetime, b.product_id, b.balance, b.balance_diff, last(pr1.price::numeric, pr1.datetime) as price1, last(pr2.price::numeric, pr2.datetime) as price2
            from investment_balance_ts b
            left join product p 
              on b.product_id = p.product_id
            -- we should have the exact price1 (share to underlying) from this exact block for all investment change
            left join price_ts pr1 
              on p.price_feed_1_id = pr1.price_feed_id 
              and pr1.datetime = b.datetime 
              and pr1.block_number = b.block_number 
            -- but for price 2 (underlying to usd) we need to match on approx time
            left join price_ts pr2 
              on p.price_feed_2_id = pr2.price_feed_id 
              and time_bucket('15min', pr2.datetime) = time_bucket('15min', b.datetime)
            where b.investor_id = %L
              and b.product_id in (select unnest(ARRAY[%L]::integer[]))
            group by 1,2,3,4
          ),
          investment_diff as (
            select b.datetime,
              p.product_key,
              p.chain,
              coalesce(p.product_data->'vault'->>'eol', p.product_data->'boost'->>'eol')::text as is_eol,
              b.price1 as share_to_underlying_price, 
              b.price2 as underlying_to_usd_price,
              b.balance as share_balance, 
              (b.balance * b.price1)::NUMERIC(100, 24) as underlying_balance,
              (b.balance * b.price1 * b.price2)::NUMERIC(100, 24) as usd_balance,
              b.balance_diff as share_diff, 
              (b.balance_diff * b.price1)::NUMERIC(100, 24) as underlying_diff,
              (b.balance_diff * b.price1 * b.price2)::NUMERIC(100, 24) as usd_diff
            from investment_diff_raw b
            join product p on p.product_id = b.product_id
          )
          select * 
          from investment_diff
          order by product_key asc, datetime asc
        `,
        [investorId, productIds],
        this.services.db,
      );
    });
  }
}

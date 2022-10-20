import Decimal from "decimal.js";
import { get, uniq } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import yargs from "yargs";
import { allChainIds, Chain } from "../../../types/chain";
import { db_query, db_query_one, db_transaction, withPgClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { fetchBlockDatetime$ } from "../connector/block-datetime";
import { DbInvestment } from "../loader/investment";
import { createRpcConfig } from "../utils/rpc-config";

const logger = rootLogger.child({ module: "beefy", component: "fix-duplicates" });

interface CmdParams {
  client: PoolClient;
  dryRun: boolean;
}

export function addDuplicateFixCmd<TOptsBefore>(yargs: yargs.Argv<TOptsBefore>) {
  return yargs.command({
    command: "duplicate-fix",
    describe: "Fix db duplicate imports",
    builder: (yargs) =>
      yargs.options({
        dryRun: { type: "boolean", demand: false, default: false, alias: "d", describe: "dry run" },
      }),
    handler: (argv): Promise<any> =>
      withPgClient(async (client) => {
        //await db_migrate();

        logger.info("Starting import script", { argv });

        const cmdParams: CmdParams = {
          client,
          dryRun: argv.dryRun,
        };

        return fixDuplicateImportedInvestmentBlocks(cmdParams);
      })(),
  });
}

interface DbInvestDuplicate {
  product_id: number;
  block_number: number;
  investor_id: number;
  datetimes: Date[];
  balances: Decimal[];
  investment_datas: object[];
}

async function fixDuplicateImportedInvestmentBlocks(cmdParams: CmdParams) {
  const pipeline$ = Rx.of(
    db_query<DbInvestDuplicate>(
      `select 
          product_id,
          block_number,
          investor_id,
          array_agg(distinct datetime) as datetimes, 
          array_agg(distinct balance::varchar) as balances,
          jsonb_agg(investment_data) as investment_datas
        from investment_balance_ts 
        group by product_id, block_number
        having count(distinct datetime) > 1
      `,
      [],
      cmdParams.client,
    ),
  ).pipe(
    Rx.concatAll(), // resolve promise
    Rx.concatAll(), // flatten array

    Rx.tap((duplicate) => logger.debug({ msg: "fixing duplicate investment", data: duplicate })),

    // hydrate the row
    Rx.map((duplicate) => ({
      ...duplicate,
      datetimes: duplicate.datetimes.map((d) => new Date(d)),
      balances: duplicate.balances.map((p) => new Decimal(p)),
    })),

    // extract the chain
    Rx.map((duplicate) => {
      let chains = duplicate.investment_datas.map((investmentData) => get(investmentData, "chain", null));
      if (chains.some((chain) => chain === null)) {
        logger.error({ msg: "duplicate investment has null chain", data: duplicate });
        throw new Error("chain not found in price data");
      }
      chains = uniq(chains);
      if (chains.length > 1) {
        logger.error({ msg: "duplicate investment has multiple chains", data: duplicate });
        throw new Error("multiple chains found in price data");
      } else if (chains.length === 0) {
        logger.error({ msg: "duplicate investment has no chains", data: duplicate });
        throw new Error("no chains found in price data");
      }

      return { duplicate, chain: chains[0] as Chain };
    }),

    // split by chain
    Rx.connect((items$) =>
      Rx.merge(
        ...allChainIds.map((chain) => {
          const ctx = {
            client: cmdParams.client,
            emitErrors: <T extends { duplicate: DbInvestDuplicate }>(item: T) => {
              logger.error({ msg: "error in fix duplicates pipeline", data: item });
              throw new Error("Error fixing duplicate");
            },
            rpcConfig: createRpcConfig(chain),
            streamConfig: {
              maxInputTake: 300,
              maxInputWaitMs: 5000,
              maxTotalRetryMs: 10_000,
              workConcurrency: 1,
            },
          };

          return items$.pipe(
            Rx.filter((item) => item.chain === chain),

            // get the real datetime for this dup
            fetchBlockDatetime$({
              ctx,
              getBlockNumber: (item) => item.duplicate.block_number,
              formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
            }),

            // find out all rows that match this duplicate
            Rx.concatMap(async (item) => {
              const res = await db_query<DbInvestment>(
                `
                select 
                 datetime as "datetime",
                 block_number as "blockNumber",
                 product_id as "productId",
                 investor_id as "investorId",
                 balance as "balance",
                 investment_data as "investmentData"
                from investment_balance_ts
                where
                  product_id = %d
                  and block_number = %d
              `,
                [item.duplicate.product_id, item.duplicate.block_number],
                cmdParams.client,
              );
              return { ...item, investmentRowsToFix: res };
            }),

            // now delete all rows for this product and datetime and re-insert with the correct datetime
            Rx.concatMap(async (item) => {
              await db_transaction(async (client) => {
                // delete any all rows that were duplicated
                await db_query(
                  `delete from investment_balance_ts where product_id = %d and block_number = %d`,
                  [item.duplicate.product_id, item.duplicate.block_number],
                  client,
                );

                // reinsert all rows, but with the correct datetime from the chain
                // one by one because there WILL be conflicts
                for (const investmentToFix of item.investmentRowsToFix) {
                  await db_query(
                    `
                    insert into investment_balance_ts (datetime, block_number, product_id, investor_id, balance, investment_data)
                      values(%s,%d,%d,%d,%s,%s)
                    ON CONFLICT (datetime, product_id, investor_id) DO UPDATE
                  `,
                  );
                }

                if (cmdParams.dryRun) {
                  throw new Error("dry run, forcing rollback");
                }
              });
              /*
              const queries = item.datetimesToDelete.map((datetimeToDelete) => ({
                sql: "delete from price_ts where product_id = %d and block_number = %d datetime = %s",
                params: [item.duplicate.product_id, item.duplicate.block_number, datetimeToDelete.toISOString()],
              }));
              for (const query of queries) {
                if (cmdParams.dryRun) {
                  const sql_w_params = pgf(query.sql, ...query.params);
                  logger.info({ msg: "dry run, not deleting rows", data: { item, query: sql_w_params } });
                } else {
                }
              }*/
              return Promise.resolve(item);
            }),
          );
        }),
      ),
    ),
  );
  logger.info({ msg: "starting duplicate fix", data: { ...cmdParams, client: "<redacted>" } });
  return consumeObservable(pipeline$);
}

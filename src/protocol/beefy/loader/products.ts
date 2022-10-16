import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { keyBy$ } from "../../../utils/rxjs/utils/key-by";
import { upsertPriceFeed$ } from "../../common/loader/price-feed";
import { upsertProduct$ } from "../../common/loader/product";
import { ErrorEmitter } from "../../common/types/import-query";
import { BatchStreamConfig } from "../../common/utils/batch-rpc-calls";
import { BeefyBoost, beefyBoostsFromGitHistory$ } from "../connector/boost-list";
import { BeefyVault, beefyVaultsFromGitHistory$ } from "../connector/vault-list";
import { normalizeVaultId } from "../utils/normalize-vault-id";

const logger = rootLogger.child({ module: "beefy", component: "import-products" });

export function importBeefyProducts$(options: { client: PoolClient }) {
  const streamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: 1,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 30 * 1000,
    maxInputTake: 500,
    // and we can affort longer retries
    maxTotalRetryMs: 30_000,
  };

  const emitVaultErrors: ErrorEmitter<BeefyVault, number> = (item) => {
    logger.error({ msg: "Error importing beefy vault product", data: { vaultId: item.target.id } });
  };
  const emitBoostErrors: ErrorEmitter<BeefyBoost, number> = (item) => {
    logger.error({ msg: "Error importing beefy boost product", data: { boostId: item.target.id } });
  };

  return Rx.pipe(
    // fetch vaults from git file history
    Rx.concatMap((chain: Chain) => {
      logger.info({ msg: "importing beefy products", data: { chain } });
      return beefyVaultsFromGitHistory$(chain).pipe(
        // create an object where we cn add attributes to safely
        Rx.map((vault) => ({ target: vault, latest: 0, range: { from: 0, to: 0 } })),
      );
    }),

    // add linked entities to the database
    Rx.pipe(
      // we have 2 "prices" for each vault
      // - balances are expressed in moo token
      // - price 1 (ppfs) converts to underlying token
      // - price 2 converts underlying to usd
      upsertPriceFeed$({
        client: options.client,
        streamConfig,
        emitErrors: emitVaultErrors,
        getFeedData: (vaultData) => {
          const vaultId = normalizeVaultId(vaultData.target.id);
          return {
            feedKey: `beefy:${vaultData.target.chain}:${vaultId}:ppfs`,
            fromAssetKey: `beefy:${vaultData.target.chain}:${vaultId}`,
            toAssetKey: `${vaultData.target.protocol}:${vaultData.target.chain}:${vaultData.target.protocol_product}`,
            priceFeedData: { active: !vaultData.target.eol, externalId: vaultId },
          };
        },
        formatOutput: (vaultData, priceFeed1) => ({ ...vaultData, priceFeed1 }),
      }),

      upsertPriceFeed$({
        client: options.client,
        streamConfig,
        emitErrors: emitVaultErrors,
        getFeedData: (vaultData) => {
          return {
            // feed key tells us that this prices comes from beefy's data
            // we may have another source of prices for the same asset
            feedKey: `beefy-data:${vaultData.target.protocol}:${vaultData.target.chain}:${vaultData.target.protocol_product}`,

            fromAssetKey: `${vaultData.target.protocol}:${vaultData.target.chain}:${vaultData.target.protocol_product}`,
            toAssetKey: "fiat:USD", // to USD
            priceFeedData: {
              active: !vaultData.target.eol,
              externalId: vaultData.target.want_price_feed_key, // the id that the data api knows
            },
          };
        },
        formatOutput: (vaultData, priceFeed2) => ({ ...vaultData, priceFeed2 }),
      }),

      upsertProduct$({
        client: options.client,
        streamConfig,
        emitErrors: emitVaultErrors,
        getProductData: (vaultData) => {
          const vaultId = normalizeVaultId(vaultData.target.id);
          const isGov = vaultData.target.is_gov_vault;
          return {
            // vault ids are unique by chain
            productKey: `beefy:vault:${vaultData.target.chain}:${vaultId}`,
            priceFeedId1: vaultData.priceFeed1.priceFeedId,
            priceFeedId2: vaultData.priceFeed2.priceFeedId,
            chain: vaultData.target.chain,
            productData: {
              type: isGov ? "beefy:gov-vault" : "beefy:vault",
              vault: vaultData.target,
            },
          };
        },
        formatOutput: (vaultData, product) => ({ ...vaultData, product }),
      }),

      Rx.tap({
        error: (err) => logger.error({ msg: "error importing chain", data: { err } }),
        complete: () => {
          logger.info({ msg: "done importing vault configs data for all chains" });
        },
      }),
    ),

    // now fetch boosts
    Rx.pipe(
      // work by chain
      Rx.groupBy((vaultData) => vaultData.target.chain),

      Rx.mergeMap(
        (vaultsByChain$) =>
          vaultsByChain$.pipe(
            // index by vault id so we can quickly ingest boosts
            keyBy$((vaultData) => normalizeVaultId(vaultData.target.id)),
            Rx.map((chainVaults) => ({
              chainVaults,
              vaultByVaultId: Object.entries(chainVaults).reduce(
                (acc, [vaultId, vaultData]) => Object.assign(acc, { [vaultId]: vaultData.target }),
                {} as Record<string, BeefyVault>,
              ),
              chain: vaultsByChain$.key,
            })),

            // fetch the boosts from git
            Rx.mergeMap(async (chainData) => {
              const chainBoosts =
                (await consumeObservable(beefyBoostsFromGitHistory$(chainData.chain, chainData.vaultByVaultId).pipe(Rx.toArray()))) || [];

              // create an object where we can add attributes to safely
              const boostsData = chainBoosts.map((boost) => {
                const vaultId = normalizeVaultId(boost.vault_id);
                if (!chainData.chainVaults[vaultId]) {
                  logger.trace({
                    msg: "vault found with id",
                    data: { vaultId, chainVaults: chainData.chainVaults },
                  });
                  throw new Error(`no price feed id for vault id ${vaultId}`);
                }
                return {
                  target: boost,
                  priceFeedId1: chainData.chainVaults[vaultId].priceFeed1.priceFeedId,
                  priceFeedId2: chainData.chainVaults[vaultId].priceFeed2.priceFeedId,
                  latest: 0,
                  range: { from: 0, to: 0 },
                };
              });
              return boostsData;
            }, 1 /* concurrency */),
            Rx.concatMap((boostsData) => Rx.from(boostsData)), // flatten

            // insert the boost as a new product
            upsertProduct$({
              client: options.client,
              streamConfig,
              emitErrors: emitBoostErrors,
              getProductData: (boostData) => {
                return {
                  productKey: `beefy:boost:${boostData.target.chain}:${boostData.target.id}`,
                  priceFeedId1: boostData.priceFeedId1,
                  priceFeedId2: boostData.priceFeedId2,
                  chain: boostData.target.chain,
                  productData: {
                    type: "beefy:boost",
                    boost: boostData.target,
                  },
                };
              },
              formatOutput: (boostData, product) => ({ ...boostData, product }),
            }),
          ),
        1 /* concurrency */,
      ),

      Rx.tap({
        error: (err) => logger.error({ msg: "error importing chain", data: { err } }),
        complete: () => {
          logger.info({ msg: "done importing boost configs data for all chains" });
        },
      }),
    ),
  );
}

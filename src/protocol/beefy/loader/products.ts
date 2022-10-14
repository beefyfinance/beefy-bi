import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { BeefyVault, beefyVaultsFromGitHistory$ } from "../connector/vault-list";
import { PoolClient } from "pg";
import { upsertPriceFeed$ } from "../../common/loader/price-feed";
import { upsertProduct$ } from "../../common/loader/product";
import { normalizeVaultId } from "../utils/normalize-vault-id";
import { beefyBoostsFromGitHistory$ } from "../connector/boost-list";
import { keyBy$ } from "../../../utils/rxjs/utils/key-by";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { rootLogger } from "../../../utils/logger";

const logger = rootLogger.child({ module: "beefy", component: "import-products" });

export function importBeefyProducts$(options: { client: PoolClient }) {
  return Rx.pipe(
    // fetch vaults from git file history
    Rx.concatMap((chain: Chain) => {
      logger.info({ msg: "importing beefy products", data: { chain } });
      return beefyVaultsFromGitHistory$(chain).pipe(
        // create an object where we cn add attributes to safely
        Rx.map((vault) => ({ vault, chain })),
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
        getFeedData: (vaultData) => {
          const vaultId = normalizeVaultId(vaultData.vault.id);
          return {
            feedKey: `beefy:${vaultData.chain}:${vaultId}:ppfs`,
            fromAssetKey: `beefy:${vaultData.chain}:${vaultId}`,
            toAssetKey: `${vaultData.vault.protocol}:${vaultData.chain}:${vaultData.vault.protocol_product}`,
            priceFeedData: { active: !vaultData.vault.eol, externalId: vaultId },
          };
        },
        formatOutput: (vaultData, priceFeed1) => ({ ...vaultData, priceFeed1 }),
      }),

      upsertPriceFeed$({
        client: options.client,
        getFeedData: (vaultData) => {
          return {
            // feed key tells us that this prices comes from beefy's data
            // we may have another source of prices for the same asset
            feedKey: `beefy-data:${vaultData.vault.protocol}:${vaultData.chain}:${vaultData.vault.protocol_product}`,

            fromAssetKey: `${vaultData.vault.protocol}:${vaultData.chain}:${vaultData.vault.protocol_product}`,
            toAssetKey: "fiat:USD", // to USD
            priceFeedData: {
              active: !vaultData.vault.eol,
              externalId: vaultData.vault.want_price_feed_key, // the id that the data api knows
            },
          };
        },
        formatOutput: (vaultData, priceFeed2) => ({ ...vaultData, priceFeed2 }),
      }),

      upsertProduct$({
        client: options.client,
        getProductData: (vaultData) => {
          const vaultId = normalizeVaultId(vaultData.vault.id);
          const isGov = vaultData.vault.is_gov_vault;
          return {
            // vault ids are unique by chain
            productKey: `beefy:vault:${vaultData.chain}:${vaultId}`,
            priceFeedId1: vaultData.priceFeed1.priceFeedId,
            priceFeedId2: vaultData.priceFeed2.priceFeedId,
            chain: vaultData.vault.chain,
            productData: {
              type: isGov ? "beefy:gov-vault" : "beefy:vault",
              vault: vaultData.vault,
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
      Rx.groupBy((vaultData) => vaultData.chain),

      Rx.mergeMap(
        (vaultsByChain$) =>
          vaultsByChain$.pipe(
            // index by vault id so we can quickly ingest boosts
            keyBy$((vaultData) => normalizeVaultId(vaultData.vault.id)),
            Rx.map((chainVaults) => ({
              chainVaults,
              vaultByVaultId: Object.entries(chainVaults).reduce(
                (acc, [vaultId, vaultData]) => Object.assign(acc, { [vaultId]: vaultData.vault }),
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
                  boost,
                  priceFeedId1: chainData.chainVaults[vaultId].priceFeed1.priceFeedId,
                  priceFeedId2: chainData.chainVaults[vaultId].priceFeed2.priceFeedId,
                };
              });
              return boostsData;
            }, 1 /* concurrency */),
            Rx.concatMap((boostsData) => Rx.from(boostsData)), // flatten

            // insert the boost as a new product
            upsertProduct$({
              client: options.client,
              getProductData: (boostData) => {
                return {
                  productKey: `beefy:boost:${boostData.boost.chain}:${boostData.boost.id}`,
                  priceFeedId1: boostData.priceFeedId1,
                  priceFeedId2: boostData.priceFeedId2,
                  chain: boostData.boost.chain,
                  productData: {
                    type: "beefy:boost",
                    boost: boostData.boost,
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

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
      upsertPriceFeed$({
        client: options.client,
        getFeedData: (vaultData) => ({
          // oracleIds are unique globally
          feedKey: `beefy:${vaultData.vault.want_price_feed_key}`,
          // we are going to express the user balance in want amount
          externalId: vaultData.vault.want_price_feed_key,
          priceFeedData: { is_active: !vaultData.vault.eol },
        }),
        formatOutput: (vaultData, priceFeed) => ({ ...vaultData, priceFeed }),
      }),

      upsertProduct$({
        client: options.client,
        getProductData: (vaultData) => {
          const vaultId = normalizeVaultId(vaultData.vault.id);
          return {
            // vault ids are unique by chain
            productKey: `beefy:vault:${vaultData.chain}:${vaultId}`,
            priceFeedId: vaultData.priceFeed.priceFeedId,
            chain: vaultData.vault.chain,
            productData: {
              type: "beefy:vault",
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
                  priceFeedId: chainData.chainVaults[vaultId].priceFeed.priceFeedId,
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
                  priceFeedId: boostData.priceFeedId,
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

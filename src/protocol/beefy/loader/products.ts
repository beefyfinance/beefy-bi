import { groupBy, keyBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { DbClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { upsertPriceFeed$ } from "../../common/loader/price-feed";
import { upsertProduct$ } from "../../common/loader/product";
import { BatchStreamConfig } from "../../common/utils/batch-rpc-calls";
import { createRpcConfig } from "../../common/utils/rpc-config";
import { BeefyBoost, beefyBoostsFromGitHistory$ } from "../connector/boost-list";
import { BeefyVault, beefyVaultsFromGitHistory$ } from "../connector/vault-list";
import { normalizeVaultId } from "../utils/normalize-vault-id";

const logger = rootLogger.child({ module: "beefy", component: "import-products" });

export function importBeefyProducts$(options: { client: DbClient }) {
  const streamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: 1,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 30 * 1000,
    maxInputTake: 500,
    dbMaxInputTake: BATCH_DB_INSERT_SIZE,
    dbMaxInputWaitMs: BATCH_MAX_WAIT_MS,
    // and we can affort longer retries
    maxTotalRetryMs: 30_000,
  };
  const ctx = {
    client: options.client,
    streamConfig,
    rpcConfig: createRpcConfig("bsc"), // we don't use it here
    chain: "bsc" as Chain, // we don't use it here
  }; //satisfies { vault: BeefyVault }; // to activate when TS 4.9 is out?

  const emitVaultError = <TObj extends { vault: BeefyVault }>(item: TObj) => {
    logger.error({ msg: "Error importing beefy vault product", data: { vaultId: item.vault.id } });
  };
  const emitBoostError = <TObj extends { boost: BeefyBoost }>(item: TObj) => {
    logger.error({ msg: "Error importing beefy boost product", data: { vaultId: item.boost.id } });
  };

  return Rx.pipe(
    // fetch vaults from git file history
    Rx.concatMap((chain: Chain) => {
      logger.info({ msg: "importing beefy products", data: { chain } });
      return beefyVaultsFromGitHistory$(chain).pipe(
        // create an object where we cn add attributes to safely
        Rx.map((vault) => ({ vault })),
      );
    }),

    // add linked entities to the database
    Rx.pipe(
      // we have 2 "prices" for each vault
      // - balances are expressed in moo token
      // - price 1 (ppfs) converts to underlying token
      // - price 2 converts underlying to usd
      upsertPriceFeed$({
        ctx,
        emitError: emitVaultError,
        getFeedData: (item) => {
          const vaultId = normalizeVaultId(item.vault.id);
          return {
            feedKey: `beefy:${item.vault.chain}:${vaultId}:ppfs`,
            fromAssetKey: `beefy:${item.vault.chain}:${vaultId}`,
            toAssetKey: `${item.vault.protocol}:${item.vault.chain}:${item.vault.protocol_product}`,
            priceFeedData: { active: !item.vault.eol, externalId: vaultId },
          };
        },
        formatOutput: (item, priceFeed1) => ({ ...item, priceFeed1 }),
      }),

      upsertPriceFeed$({
        ctx,
        emitError: emitVaultError,
        getFeedData: (item) => {
          return {
            // feed key tells us that this prices comes from beefy's data
            // we may have another source of prices for the same asset
            feedKey: `beefy-data:${item.vault.protocol}:${item.vault.chain}:${item.vault.protocol_product}`,

            fromAssetKey: `${item.vault.protocol}:${item.vault.chain}:${item.vault.protocol_product}`,
            toAssetKey: "fiat:USD", // to USD
            priceFeedData: {
              active: !item.vault.eol,
              externalId: item.vault.want_price_feed_key, // the id that the data api knows
            },
          };
        },
        formatOutput: (item, priceFeed2) => ({ ...item, priceFeed2 }),
      }),

      upsertProduct$({
        ctx,
        emitError: emitVaultError,
        getProductData: (item) => {
          const vaultId = normalizeVaultId(item.vault.id);
          const isGov = item.vault.is_gov_vault;
          return {
            // vault ids are unique by chain
            productKey: `beefy:vault:${item.vault.chain}:${vaultId}`,
            priceFeedId1: item.priceFeed1.priceFeedId,
            priceFeedId2: item.priceFeed2.priceFeedId,
            chain: item.vault.chain,
            productData: {
              type: isGov ? "beefy:gov-vault" : "beefy:vault",
              vault: item.vault,
            },
          };
        },
        formatOutput: (item, product) => ({ ...item, product }),
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
      Rx.pipe(
        Rx.toArray(),
        Rx.map((vaults) => Object.entries(groupBy(vaults, (vault) => vault.vault.chain)).map(([chain, items]) => ({ chain: chain as Chain, items }))),
        Rx.concatAll(),
      ),

      // fetch the boosts from git
      Rx.concatMap(async ({ items, chain }) => {
        const chainVaults = items.map((item) => item.vault);
        const boostObs = beefyBoostsFromGitHistory$(chain, chainVaults).pipe(Rx.toArray());
        const chainBoosts = (await consumeObservable(boostObs)) ?? [];
        const itemsByVaultId = keyBy(items, (item) => normalizeVaultId(item.vault.id));

        // create an object where we can add attributes to safely
        const boostsData = chainBoosts.map((boost) => {
          const vaultId = normalizeVaultId(boost.vault_id);
          if (!itemsByVaultId[vaultId]) {
            logger.error({ msg: "vault not found with id", data: { vaultId, boostId: boost.id } });
            logger.debug({ msg: "available vaults", data: { chainVaultIds: Object.keys(itemsByVaultId) } });
            throw new Error(`no price feed id for vault id ${vaultId}`);
          }
          return {
            boost,
            priceFeedId1: itemsByVaultId[vaultId].priceFeed1.priceFeedId,
            priceFeedId2: itemsByVaultId[vaultId].priceFeed2.priceFeedId,
          };
        });
        return boostsData;
      }),
      Rx.concatMap((item) => Rx.from(item)), // flatten

      // insert the boost as a new product
      upsertProduct$({
        ctx,
        emitError: emitBoostError,
        getProductData: (item) => {
          return {
            productKey: `beefy:boost:${item.boost.chain}:${item.boost.id}`,
            priceFeedId1: item.priceFeedId1,
            priceFeedId2: item.priceFeedId2,
            chain: item.boost.chain,
            productData: {
              type: "beefy:boost",
              boost: item.boost,
            },
          };
        },
        formatOutput: (item, product) => ({ ...item, product }),
      }),

      Rx.tap({
        error: (err) => logger.error({ msg: "error importing chain", data: { err } }),
        complete: () => {
          logger.info({ msg: "done importing boost configs data for all chains" });
        },
      }),
    ),
  );
}

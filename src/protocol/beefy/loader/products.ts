import { groupBy, keyBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { getChainWNativeTokenSymbol } from "../../../utils/addressbook";
import { DbClient } from "../../../utils/db";
import { mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { upsertIgnoreAddress$ } from "../../common/loader/ignore-address";
import { upsertPriceFeed$ } from "../../common/loader/price-feed";
import { upsertProduct$ } from "../../common/loader/product";
import { ErrorReport, ImportCtx } from "../../common/types/import-context";
import { computeIsDashboardEOL } from "../../common/utils/eol";
import { NoRpcRunnerConfig, createChainRunner } from "../../common/utils/rpc-chain-runner";
import { BeefyBoost, beefyBoostsFromGitHistory$ } from "../connector/boost-list";
import { BeefyVault, beefyVaultsFromGitHistory$, isBeefyBridgedVersionOfStdVaultConfig, isBeefyGovVaultConfig } from "../connector/vault-list";
import { normalizeVaultId } from "../utils/normalize-vault-id";

const logger = rootLogger.child({ module: "beefy", component: "import-products" });

export function createBeefyProductRunner(options: { client: DbClient; runnerConfig: NoRpcRunnerConfig<Chain> }) {
  const emitVaultError = <TObj extends { vault: BeefyVault }>(item: TObj, report: ErrorReport) => {
    logger.error(mergeLogsInfos({ msg: "Error importing beefy vault product", data: { vaultId: item.vault.id } }, report.infos));
    logger.error(report);
  };
  const emitBoostError = <TObj extends { boost: BeefyBoost }>(item: TObj, report: ErrorReport) => {
    logger.error(mergeLogsInfos({ msg: "Error importing beefy boost product", data: { vaultId: item.boost.id } }, report.infos));
    logger.error(report);
  };

  const createPipeline = (ctx: ImportCtx) =>
    Rx.pipe(
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
        // we have 2 or 3 "prices" for each vault
        // - balances are expressed in moo token
        // - price 1 (ppfs) converts to underlying token
        // - price 2 converts underlying to usd
        // - boosts and vaults have a reward token
        upsertPriceFeed$({
          ctx,
          emitError: emitVaultError,
          getFeedData: (item) => {
            // we want to use the same price feed for all versions of the same product
            const vault = item.vault.bridged_version_of ?? item.vault;
            const vaultId = normalizeVaultId(vault.id);
            return {
              feedKey: `beefy:${vault.chain}:${vaultId}:ppfs`,
              fromAssetKey: `beefy:${vault.chain}:${vaultId}`, // from the vault
              toAssetKey: `${vault.protocol}:${vault.chain}:${vault.protocol_product}`, // to underlying amount
              priceFeedData: { active: !vault.eol, externalId: vaultId },
            };
          },
          formatOutput: (item, priceFeed1) => ({ ...item, priceFeed1 }),
        }),

        upsertPriceFeed$({
          ctx,
          emitError: emitVaultError,
          getFeedData: (item) => {
            // we want to use the same price feed for all versions of the same product
            const vault = item.vault.bridged_version_of ?? item.vault;
            return {
              // feed key tells us that this prices comes from beefy's data
              // we may have another source of prices for the same asset
              feedKey: `beefy-data:${vault.protocol}:${vault.chain}:${vault.protocol_product}`,
              fromAssetKey: `${vault.protocol}:${vault.chain}:${vault.protocol_product}`, // from underlying amount
              toAssetKey: "fiat:USD", // to USD
              priceFeedData: {
                active: !computeIsDashboardEOL(ctx.behaviour, vault.eol, vault.eol_date),
                externalId: vault.want_price_feed_key, // the id that the data api knows
              },
            };
          },
          formatOutput: (item, priceFeed2) => ({ ...item, priceFeed2 }),
        }),

        // add pendingRewardsPriceFeedId field
        Rx.connect((items$) =>
          Rx.merge(
            // standard vaults do not have pending rewards
            items$.pipe(
              Rx.filter((item) => !item.vault.is_gov_vault),
              Rx.map((item) => ({ ...item, pendingRewardsPriceFeed: null })),
            ),
            items$.pipe(
              Rx.filter((item) => item.vault.is_gov_vault),

              upsertPriceFeed$({
                ctx,
                emitError: emitVaultError,
                getFeedData: (item) => {
                  const vault = item.vault.bridged_version_of ?? item.vault;
                  // gov vaults are rewarded in gas token
                  const rewardToken = getChainWNativeTokenSymbol(vault.chain);
                  return {
                    // feed key tells us that this prices comes from beefy's data
                    // we may have another source of prices for the same asset
                    feedKey: `beefy-data:${vault.chain}:${rewardToken}`,

                    fromAssetKey: `${vault.chain}:${rewardToken}`,
                    toAssetKey: "fiat:USD", // to USD
                    priceFeedData: {
                      active: !computeIsDashboardEOL(ctx.behaviour, vault.eol, vault.eol_date),
                      externalId: rewardToken, // the id that the data api knows
                    },
                  };
                },
                formatOutput: (item, pendingRewardsPriceFeed) => ({ ...item, pendingRewardsPriceFeed }),
              }),
            ),
          ),
        ),

        upsertProduct$({
          ctx,
          emitError: emitVaultError,
          getProductData: (item) => {
            const dashboardEol = computeIsDashboardEOL(ctx.behaviour, item.vault.eol, item.vault.eol_date);
            const productData = isBeefyBridgedVersionOfStdVaultConfig(item.vault)
              ? {
                  type: "beefy:bridged-vault" as const,
                  dashboardEol,
                  vault: item.vault,
                }
              : isBeefyGovVaultConfig(item.vault)
              ? {
                  type: "beefy:gov-vault" as const,
                  dashboardEol,
                  vault: item.vault,
                }
              : {
                  type: "beefy:vault" as const,
                  dashboardEol,
                  vault: item.vault,
                };
            return {
              // vault ids are unique by chain
              productKey: `beefy:vault:${item.vault.chain}:${item.vault.contract_address.toLocaleLowerCase()}`,
              priceFeedId1: item.priceFeed1.priceFeedId,
              priceFeedId2: item.priceFeed2.priceFeedId,
              pendingRewardsPriceFeedId: item.pendingRewardsPriceFeed?.priceFeedId || null,
              chain: item.vault.chain,
              productData: productData,
            };
          },
          formatOutput: (item, product) => ({ ...item, product }),
        }),

        upsertIgnoreAddress$({
          ctx,
          emitError: (err) => logger.error({ msg: "error adding vault ignore address", data: { err } }),
          getIgnoreAddressData: (item) => ({ address: item.vault.contract_address, chain: item.vault.chain }),
          formatOutput: (product) => product,
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
          Rx.map((vaults) =>
            Object.entries(groupBy(vaults, (vault) => vault.vault.chain)).map(([chain, items]) => ({ chain: chain as Chain, items })),
          ),
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

        // insert the reward token price feed
        upsertPriceFeed$({
          ctx,
          emitError: emitBoostError,
          getFeedData: (item) => {
            // gov vaults are rewarded in gas token
            const rewardToken = item.boost.reward_token_price_feed_key;
            return {
              // feed key tells us that this prices comes from beefy's data
              // we may have another source of prices for the same asset
              feedKey: `beefy-data:${item.boost.chain}:${rewardToken}`,

              fromAssetKey: `${item.boost.chain}:${rewardToken}`,
              toAssetKey: "fiat:USD", // to USD
              priceFeedData: {
                active: !computeIsDashboardEOL(ctx.behaviour, item.boost.eol, item.boost.eol_date),
                externalId: rewardToken, // the id that the data api knows
              },
            };
          },
          formatOutput: (item, pendingRewardsPriceFeed) => ({ ...item, pendingRewardsPriceFeed }),
        }),

        // insert the boost as a new product
        upsertProduct$({
          ctx,
          emitError: emitBoostError,
          getProductData: (item) => {
            return {
              productKey: `beefy:boost:${item.boost.chain}:${item.boost.contract_address.toLocaleLowerCase()}`,
              priceFeedId1: item.priceFeedId1,
              priceFeedId2: item.priceFeedId2,
              pendingRewardsPriceFeedId: item.pendingRewardsPriceFeed?.priceFeedId || null,
              chain: item.boost.chain,
              productData: {
                type: "beefy:boost",
                dashboardEol: computeIsDashboardEOL(ctx.behaviour, item.boost.eol, item.boost.eol_date),
                boost: item.boost,
              },
            };
          },
          formatOutput: (item, product) => ({ ...item, product }),
        }),

        // add the boost to the list of ignored addresses
        upsertIgnoreAddress$({
          ctx,
          emitError: (err) => logger.error({ msg: "error adding boost ignore address", data: { err } }),
          getIgnoreAddressData: (item) => ({ address: item.boost.contract_address, chain: item.boost.chain }),
          formatOutput: (product) => product,
        }),

        Rx.tap({
          error: (err) => logger.error({ msg: "error importing chain", data: { err } }),
          complete: () => {
            logger.info({ msg: "done importing boost configs data for all chains" });
          },
        }),
      ),
    );

  return createChainRunner(options.runnerConfig, createPipeline);
}

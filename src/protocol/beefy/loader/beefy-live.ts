import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { withPgClient } from "../../../utils/db";
import { BeefyVault, beefyVaultsFromGitHistory$ } from "../connector/vault-list";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger";
import { ethers } from "ethers";
import { cloneDeep, sample } from "lodash";
import { RPC_URLS } from "../../../utils/config";
import { fetchErc20Transfers$, fetchERC20TransferToAStakingContract$ } from "../../common/connector/erc20-transfers";
import { fetchBeefyPrices } from "../connector/prices";
import { loaderByChain$ } from "../../common/loader/loader-by-chain";
import Decimal from "decimal.js";
import { normalizeAddress } from "../../../utils/ethers";
import { priceFeedList$, upsertPriceFeed$ } from "../../common/loader/price-feed";
import { DbBeefyProduct, DbProduct, productList$, upsertProduct$ } from "../../common/loader/product";
import { normalizeVaultId } from "../utils/normalize-vault-id";
import { findMissingPriceRangeInDb$, upsertPrices$ } from "../../common/loader/prices";
import { addHistoricalBlockQuery$, addLatestBlockQuery$ } from "../../common/connector/block-query";
import { fetchBeefyPPFS$ } from "../connector/ppfs";
import { beefyBoostsFromGitHistory$ } from "../connector/boost-list";
import { addMissingImportStatus, updateImportStatus } from "../../common/loader/import-status";
import { keyBy$ } from "../../../utils/rxjs/utils/key-by";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { sleep } from "../../../utils/async";
import yargs from "yargs";
import { rangeExclude, rangeMerge } from "../../../utils/range";
import { loadTransfers$, TransferWithRate } from "./load-transfers";
import { bufferUntilKeyChanged } from "../../../utils/rxjs/utils/buffer-until-key-change";
import { ErrorEmitter, ProductImportQuery } from "../../common/types/product-query";
import { BatchStreamConfig } from "../../common/utils/batch-rpc-calls";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { isBeefyBoostProductImportQuery, isBeefyGovVaultProductImportQuery, isBeefyStandardVaultProductImportQuery } from "../utils/type-guard";
import { ProgrammerError } from "../../../utils/rxjs/utils/programmer-error";
import { bufferUntilBelowMachineThresholds } from "../../../utils/rxjs/utils/buffer-until-below-machine-threshold";

const logger = rootLogger.child({ module: "import-script", component: "beefy-live" });

// remember the last imported block number for each chain so we can reduce the amount of data we fetch
type ImportState = {
  [key in Chain]: { lastImportedBlockNumber: number | null };
};
const importState: ImportState = allChainIds.reduce(
  (agg, chain) => Object.assign(agg, { [chain]: { lastImportedBlockNumber: null } }),
  {} as ImportState,
);

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: false, default: "all", describe: "only import data for this chain" },
      contractAddress: { type: "string", demand: false, alias: "a", describe: "only import data for this contract address" },
      currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "Force the current block number" },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const filterChains = chain === "all" ? allChainIds : [chain];
  const filterContractAddress = argv.contractAddress || null;
  const forceCurrentBlockNumber = argv.currentBlockNumber || null;

  if (forceCurrentBlockNumber !== null && chain === "all") {
    throw new ProgrammerError("Cannot force current block number without a chain filter");
  }

  logger.trace({ msg: "starting", data: { filterChains, filterContractAddress } });

  // ---
  // from time to time, refresh vaults
  //
  // ---
  // create the insert pipeline for transfers
  // register logs for all contracts
  // emit to the pipeline on new log
  //

  const pollLiveData = withPgClient(async (client: PoolClient) => {
    const process = (chain: Chain) => (input$: Rx.Observable<DbProduct>) =>
      input$.pipe(importChainRecentData(client, chain, forceCurrentBlockNumber));
    const pipeline$ = productList$(client, "beefy").pipe(
      // apply command filters
      Rx.filter((product) => filterChains.includes(product.chain)),
      Rx.filter(
        (product) =>
          filterContractAddress === null ||
          (product.productData.type === "beefy:vault"
            ? product.productData.vault.contract_address.toLocaleLowerCase() === filterContractAddress.toLocaleLowerCase()
            : product.productData.boost.contract_address.toLocaleLowerCase() === filterContractAddress.toLocaleLowerCase()),
      ),

      loaderByChain$(process),
    );
    return consumeObservable(pipeline$);
  });
  const pollBeefyProducts = withPgClient(loadBeefyProducts);
  const pollPriceData = withPgClient(fetchPrices);

  const backfillHistory = withPgClient(async (client: PoolClient) => {
    const process = (chain: Chain) => (input$: Rx.Observable<DbProduct>) =>
      input$.pipe(importChainHistoricalData(client, chain, forceCurrentBlockNumber));

    const pipeline$ = productList$(client, "beefy").pipe(
      // apply command filters
      Rx.filter((product) => filterChains.includes(product.chain)),
      Rx.filter(
        (product) =>
          filterContractAddress === null ||
          (product.productData.type === "beefy:vault"
            ? product.productData.vault.contract_address.toLocaleLowerCase() === filterContractAddress.toLocaleLowerCase()
            : product.productData.boost.contract_address.toLocaleLowerCase() === filterContractAddress.toLocaleLowerCase()),
      ),

      // some backpressure mechanism
      bufferUntilBelowMachineThresholds({
        checkIntervalMs: 1000,
        maxMemoryThresholdMb: 500,
      }),

      // load  historical data
      loaderByChain$(process),
    );
    return consumeObservable(pipeline$);
  });

  return new Promise(async () => {
    // start polling live data immediately
    //await pollBeefyProducts();
    while (true) {
      await backfillHistory();
      //await pollLiveData();
      await sleep(5_000);
    }

    //await pollLiveData();
    //await pollPriceData();
    ////
    ////// then start polling at regular intervals
    //setInterval(pollLiveData, samplingPeriodMs["30s"]);
    ////setInterval(pollBeefyProducts, samplingPeriodMs["1day"]);
    //setInterval(pollPriceData, samplingPeriodMs["5min"]);
  });
}

runMain(main);

function importChainHistoricalData(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  const provider = new ethers.providers.JsonRpcProvider({
    url: sample(RPC_URLS[chain]) as string,
    // set a low timeout otherwise ethers keeps all call data in memory until the timeout is reached
    timeout: 30_000,
  });

  const streamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: 1,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 5 * 60 * 1000,
    maxInputTake: 500,
    // and we can affort longer retries
    maxTotalRetryMs: 30_000,
  };
  const {
    observable: productErrors$,
    next: emitErrors,
    complete: completeProductErrors$,
  } = createObservableWithNext<ProductImportQuery<DbProduct>>();

  return Rx.pipe(
    // add typings to the input item
    Rx.filter((_: DbBeefyProduct) => true),

    addMissingImportStatus({
      client,
      chain,
      provider,
      getContractAddress: (product) =>
        product.productData.type === "beefy:vault" ? product.productData.vault.contract_address : product.productData.boost.contract_address,
      getInitialImportData: (_, contractCreationInfo) => ({
        type: "beefy",
        data: {
          contractCreatedAtBlock: contractCreationInfo.blockNumber,
          coveredBlockRange: {
            from: contractCreationInfo.blockNumber,
            to: contractCreationInfo.blockNumber,
          },
          blockRangesToRetry: [],
        },
      }),
    }),

    // generate the block ranges to import
    addHistoricalBlockQuery$({
      client,
      chain,
      provider,
      forceCurrentBlockNumber,
      streamConfig,
      getImportStatus: (item) => item.importStatus,
      formatOutput: (item, blockQueries) => ({ ...item, blockQueries }),
    }),

    // convert to stream of product queries
    Rx.concatMap((item) =>
      item.blockQueries.map((blockRange): ProductImportQuery<DbBeefyProduct> => {
        const { blockQueries, ...rest } = item;
        return { ...rest, blockRange };
      }),
    ),

    // process the queries
    fetchAndInsertBeefyProductRange$({
      client,
      chain,
      streamConfig,
      provider,
      emitErrors: emitErrors,
    }),

    // handle the results
    Rx.pipe(
      // make sure we close the errors observable when we are done
      Rx.finalize(() => setTimeout(completeProductErrors$, 1000)),
      // merge the errors back in, all items here should have been successfully treated
      Rx.mergeWith(productErrors$),
      Rx.map((item) => ({ ...item, success: "success" in item ? item.success : false })),
    ),

    Rx.pipe(
      // merge the statuses ranges together to call updateImportStatus less often
      // but flush often enough so we don't go too long before updating the status
      bufferUntilKeyChanged((item) => `${item.product.productId}`, streamConfig.maxInputTake),

      // update the import status with the new block range
      updateImportStatus({
        client,
        getProductId: (items) => items[0].product.productId,
        mergeImportStatus: (items, importStatus) => {
          const productId = items[0].product.productId;
          const productKey = items[0].product.productKey;
          if (importStatus.importData.type !== "beefy") {
            throw new Error(`Import status is not for beefy: ${importStatus.importData.type}`);
          }

          const blockRanges = items.map((item) => item.blockRange);
          const newImportStatus = cloneDeep(importStatus);

          // either way, we covered this range, we need to remember that
          const mergedRange = rangeMerge([newImportStatus.importData.data.coveredBlockRange, ...blockRanges]);
          newImportStatus.importData.data.coveredBlockRange = mergedRange[0]; // only take the first one
          if (mergedRange.length > 1) {
            logger.warn({
              msg: "Unexpectedly merged multiple block ranges",
              data: { productId, blockRanges, mergedRange, importStatus: newImportStatus },
            });
          }

          for (const item of items) {
            const blockRange = item.blockRange;
            if (item.success) {
              // remove the retried range if present
              newImportStatus.importData.data.blockRangesToRetry = newImportStatus.importData.data.blockRangesToRetry.flatMap((range) =>
                rangeExclude(range, blockRange),
              );
            } else {
              // add it if not present
              newImportStatus.importData.data.blockRangesToRetry.push(blockRange);
            }
          }

          // merge the ranges to retry to avoid duplicates
          newImportStatus.importData.data.blockRangesToRetry = rangeMerge(newImportStatus.importData.data.blockRangesToRetry);

          const rangeResults = items.map((item) => ({ range: item.blockRange, success: item.success }));
          logger.trace({
            msg: "Updating import status",
            data: {
              successRanges: rangeMerge(rangeResults.filter((item) => item.success).map((item) => item.range)),
              errorRanges: rangeMerge(rangeResults.filter((item) => !item.success).map((item) => item.range)),
              product: { productId, productKey },
              importStatus,
              newImportStatus,
            },
          });

          return newImportStatus;
        },
        formatOutput: (items, importStatusUpdated) => items.map((item) => ({ ...item, importStatusUpdated })),
      }),

      // flatten the items
      Rx.mergeAll(),

      // logging
      Rx.tap((item) => {
        if (!item.success) {
          logger.trace({ msg: "Failed to import historical data", data: { productId: item.product.productData, blockRange: item.blockRange } });
        }
      }),

      Rx.finalize(() => logger.info({ msg: "Finished importing historical data", data: { chain } })),
    ),
  );
}

function importChainRecentData(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  const provider = new ethers.providers.JsonRpcProvider({
    url: sample(RPC_URLS[chain]) as string,
    // set a low timeout otherwise ethers keeps all call data in memory until the timeout is reached
    timeout: 10_000,
  });

  const streamConfig: BatchStreamConfig = {
    // since we are doing live data on a small amount of queries (one per vault)
    // we can afford some amount of concurrency
    workConcurrency: 10,
    // But we can not afford to wait before processing the next batch
    maxInputWaitMs: 5_000,
    maxInputTake: 500,
    // and we cannot afford too long of a retry per product
    maxTotalRetryMs: 10_000,
  };
  const { observable: productErrors$, next: emitErrors } = createObservableWithNext<ProductImportQuery<DbProduct>>();

  return Rx.pipe(
    // add typings to the input item
    Rx.filter((_: DbBeefyProduct) => true),

    // create an object we can safely add data to
    Rx.map((product) => ({ product })),

    // only live boosts and vaults
    Rx.filter(({ product }) =>
      product.productData.type === "beefy:vault" ? product.productData.vault.eol === false : product.productData.boost.eol === false,
    ),

    // find out the blocks we want to query
    addLatestBlockQuery$({
      chain,
      provider,
      forceCurrentBlockNumber,
      streamConfig: streamConfig,
      getLastImportedBlock: () => importState[chain].lastImportedBlockNumber ?? null,
      formatOutput: (item, blockRange) => ({ ...item, blockRange }),
    }),

    // process the queries
    fetchAndInsertBeefyProductRange$({ client, chain, streamConfig, provider, emitErrors }),

    // merge the errors back in, all items here should have been successfully treated
    Rx.pipe(
      Rx.mergeWith(productErrors$),
      Rx.map((item) => ({ ...item, success: "success" in item ? item.success : false })),

      // logging
      Rx.tap((item) => {
        if (item.success) {
          logger.debug({
            msg: "Imported live data",
            data: { productId: item.product.productData, blockRange: item.blockRange, success: item.success },
          });
        } else {
          logger.error({ msg: "Failed to import live data", data: { productId: item.product.productData, blockRange: item.blockRange } });
        }
      }),
      Rx.finalize(() => logger.info({ msg: "Finished importing live data", data: { chain } })),
    ),
  );
}

function fetchAndInsertBeefyProductRange$(options: {
  client: PoolClient;
  provider: ethers.providers.JsonRpcProvider;
  chain: Chain;
  emitErrors: ErrorEmitter;
  streamConfig: BatchStreamConfig;
}) {
  /*
  provider.on(
    "debug",
    (
      event:
        | { action: "request"; request: any }
        | {
            action: "requestBatch";
            request: any;
          }
        | {
            action: "response";
            request: any;
            response: any;
          }
        | {
            action: "response";
            error: any;
            request: any;
          },
    ) => {
      if (event.action === "request") {
        logger.trace({ msg: "RPC request", data: { request: event.request } });
      } else if (event.action === "response" && "response" in event) {
        logger.trace({ msg: "RPC response", data: { request: event.request, response: event.response } });
      } else if (event.action === "response" && "error" in event) {
        logger.error({ msg: "RPC error", data: { request: event.request, error: event.error } });
      }
    },
  );*/

  const boostTransfers$ = Rx.pipe(
    // set the right product type
    Rx.filter(isBeefyBoostProductImportQuery),

    // fetch latest transfers from and to the boost contract
    fetchERC20TransferToAStakingContract$({
      provider: options.provider,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getQueryParams: (item) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const boost = item.product.productData.boost;
        return {
          address: boost.staked_token_address,
          decimals: boost.staked_token_decimals,
          trackAddress: boost.contract_address,
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    // then we need the vault ppfs to interpret the balance
    fetchBeefyPPFS$({
      provider: options.provider,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getPPFSCallParams: (item) => {
        const boost = item.product.productData.boost;
        return {
          vaultAddress: boost.staked_token_address,
          underlyingDecimals: item.product.productData.boost.vault_want_decimals,
          vaultDecimals: boost.staked_token_decimals,
          blockNumbers: item.transfers.map((t) => t.blockNumber),
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, ppfss) => ({ ...item, ppfss }),
    }),

    // add an ignore address so we can pipe this observable into the main pipeline again
    Rx.map((item) => ({ ...item, ignoreAddresses: [item.product.productData.boost.contract_address] })),
  );

  const standardVaultTransfers$ = Rx.pipe(
    // set the right product type
    Rx.filter(isBeefyStandardVaultProductImportQuery),

    // for standard vaults, we only ignore the mint-burn addresses
    Rx.map((item) => ({
      ...item,
      ignoreAddresses: [normalizeAddress("0x0000000000000000000000000000000000000000")],
    })),

    // fetch the vault transfers
    fetchErc20Transfers$({
      provider: options.provider,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getQueryParams: (item) => {
        const vault = item.product.productData.vault;
        return {
          address: vault.contract_address,
          decimals: vault.token_decimals,
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    // fetch the ppfs
    fetchBeefyPPFS$({
      provider: options.provider,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getPPFSCallParams: (item) => {
        return {
          vaultAddress: item.product.productData.vault.contract_address,
          underlyingDecimals: item.product.productData.vault.want_decimals,
          vaultDecimals: item.product.productData.vault.token_decimals,
          blockNumbers: item.transfers.map((t) => t.blockNumber),
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, ppfss) => ({ ...item, ppfss }),
    }),
  );

  const govVaultTransfers$ = Rx.pipe(
    // set the right product type
    Rx.filter(isBeefyGovVaultProductImportQuery),

    // for gov vaults, we ignore the vault address and the associated maxi vault to avoid double counting
    // todo: ignore the maxi vault
    Rx.map((item) => ({
      ...item,
      ignoreAddresses: [
        normalizeAddress("0x0000000000000000000000000000000000000000"),
        normalizeAddress(item.product.productData.vault.contract_address),
      ],
    })),

    fetchERC20TransferToAStakingContract$({
      provider: options.provider,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getQueryParams: (item) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const vault = item.product.productData.vault;
        return {
          address: vault.want_address,
          decimals: vault.want_decimals,
          trackAddress: vault.contract_address,
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    // simulate a ppfs of 1 so we can treat gov vaults like standard vaults
    Rx.map((item) => ({
      ...item,
      ppfss: item.transfers.map(() => new Decimal(1)),
    })),
  );

  const importTransfers$ = loadTransfers$({
    chain: options.chain,
    client: options.client,
    streamConfig: options.streamConfig,
    emitErrors: options.emitErrors,
    provider: options.provider,
  });

  return Rx.pipe(
    // add typings to the input item
    Rx.filter((_: ProductImportQuery<DbBeefyProduct>) => true),

    // create groups of products to import
    Rx.groupBy((item) =>
      item.product.productData.type !== "beefy:vault" ? "boost" : item.product.productData.vault.is_gov_vault ? "gov-vault" : "standard-vault",
    ),
    Rx.mergeMap((productType$) => {
      if (productType$.key === "boost") {
        return productType$.pipe(boostTransfers$);
      } else if (productType$.key === "standard-vault") {
        return productType$.pipe(standardVaultTransfers$);
      } else if (productType$.key === "gov-vault") {
        return productType$.pipe(govVaultTransfers$);
      } else {
        throw new ProgrammerError(`Unhandled product type: ${productType$.key}`);
      }
    }),

    Rx.tap((item) => {
      if (item.transfers.length > 0) {
        logger.debug({
          msg: "Got transfers for product",
          data: { productId: item.product.productId, blockRange: item.blockRange, transferCount: item.transfers.length },
        });

        // add some verification about the transfers
        if (process.env.NODE_ENV === "development") {
          for (const transfer of item.transfers) {
            if (transfer.blockNumber < item.blockRange.from || transfer.blockNumber > item.blockRange.to) {
              logger.error({
                msg: "Transfer out of requested block range",
                data: { productId: item.product.productId, blockRange: item.blockRange, transferBlock: transfer.blockNumber, transfer },
              });
            }
          }
        }
      }
    }),

    // work by batches of 300 block range
    Rx.bufferCount(300),
    Rx.mergeMap((items) => {
      const itemsTransfers = items.map((item) =>
        item.transfers.map(
          (transfer, idx): TransferWithRate => ({
            transfer,
            ignoreAddresses: item.ignoreAddresses,
            product: item.product,
            sharesRate: item.ppfss[idx],
            blockRange: item.blockRange,
          }),
        ),
      );

      return Rx.of(itemsTransfers.flat()).pipe(
        Rx.mergeAll(),

        Rx.tap((item) =>
          logger.trace({
            msg: "importing transfer",
            data: { product: item.product.productId, blockRange: item.blockRange, transfer: item.transfer },
          }),
        ),

        // enhance transfer and insert in database
        importTransfers$,

        // return to product representation
        Rx.count(),
        Rx.map(() => items.map((item) => ({ ...item, success: true }))),
      );
    }, options.streamConfig.workConcurrency),
    Rx.mergeAll(), // flatten items
  );
}

function loadBeefyProducts(client: PoolClient) {
  logger.info({ msg: "importing beefy products" });

  const vaultPipeline$ = Rx.of(...allChainIds)
    .pipe(
      // fetch vaults from git file history
      Rx.concatMap((chain) =>
        beefyVaultsFromGitHistory$(chain).pipe(
          // create an object where we cn add attributes to safely
          Rx.map((vault) => ({ vault, chain })),
        ),
      ),

      // we are going to express the user balance in want amount
      upsertPriceFeed$({
        client,
        getFeedData: (vaultData) => ({
          // oracleIds are unique globally
          feedKey: `beefy:${vaultData.vault.want_price_feed_key}`,
          externalId: vaultData.vault.want_price_feed_key,
          priceFeedData: { is_active: !vaultData.vault.eol },
        }),
        formatOutput: (vaultData, priceFeed) => ({ ...vaultData, priceFeed }),
      }),

      upsertProduct$({
        client,
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
    )
    .pipe(
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
              client,
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
    );

  return consumeObservable(vaultPipeline$);
}

function fetchPrices(client: PoolClient) {
  logger.info({ msg: "fetching vault prices" });

  const pipeline$ = priceFeedList$(client, "beefy")
    .pipe(
      Rx.tap((priceFeed) => logger.debug({ msg: "fetching beefy prices", data: priceFeed })),

      // only live price feeds
      Rx.filter((priceFeed) => priceFeed.priceFeedData.is_active),

      // map to an object where we can add attributes to safely
      Rx.map((priceFeed) => ({ priceFeed })),

      // remove duplicates
      Rx.distinct(({ priceFeed }) => priceFeed.externalId),

      // find which data is missing
      findMissingPriceRangeInDb$({
        client,
        getFeedId: (productData) => productData.priceFeed.priceFeedId,
        formatOutput: (productData, missingData) => ({ ...productData, missingData }),
      }),
    )
    .pipe(
      // be nice with beefy api plz
      rateLimit$(300),

      // now we fetch
      Rx.mergeMap(async (productData) => {
        const debugLogData = {
          priceFeedKey: productData.priceFeed.feedKey,
          priceFeed: productData.priceFeed.priceFeedId,
          missingData: productData.missingData,
        };

        logger.debug({ msg: "fetching prices", data: debugLogData });
        const prices = await fetchBeefyPrices("15min", productData.priceFeed.externalId, {
          startDate: productData.missingData.fromDate,
          endDate: productData.missingData.toDate,
        });
        logger.debug({ msg: "got prices", data: { ...debugLogData, priceCount: prices.length } });

        return { ...productData, prices };
      }, 1 /* concurrency */),

      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.concatMap((productData) => Rx.from(productData.prices.map((price) => ({ ...productData, price })))),

      upsertPrices$({
        client,
        getPriceData: (priceData) => ({
          datetime: priceData.price.datetime,
          priceFeedId: priceData.priceFeed.priceFeedId,
          usdValue: new Decimal(priceData.price.value),
        }),
        formatOutput: (priceData, price) => ({ ...priceData, price }),
      }),
    );

  return consumeObservable(pipeline$).then(() => logger.info({ msg: "done fetching prices" }));
}

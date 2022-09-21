import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { withPgClient } from "../../../utils/db";
import { BeefyVault, beefyVaultsFromGitHistory$ } from "../connector/vault-list";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger";
import { consumeObservable } from "../../../utils/observable";
import { ethers } from "ethers";
import { curry, sample } from "lodash";
import { RPC_URLS } from "../../../utils/config";
import { fetchErc20Transfers$, fetchERC20TransferToAStakingContract$ } from "../../common/connector/erc20-transfers";
import { fetchERC20TokenBalance$ } from "../../common/connector/owner-balance";
import { fetchBlockDatetime$ } from "../../common/connector/block-datetime";
import { fetchBeefyPrices } from "../connector/prices";
import { loaderByChain$ } from "../../common/loader/loader-by-chain";
import Decimal from "decimal.js";
import { normalizeAddress } from "../../../utils/ethers";
import { handleRpcErrors$ } from "../../common/connector/rpc-errors";
import { priceFeedList$, upsertPriceFeed$ } from "../../common/loader/price-feed";
import {
  DbBeefyBoostProduct,
  DbBeefyProduct,
  DbBeefyVaultProduct,
  productList$,
  upsertProduct$,
} from "../../common/loader/product";
import { normalizeVaultId } from "../utils/normalize-vault-id";
import { findMissingPriceRangeInDb$, upsertPrices$ } from "../../common/loader/prices";
import { upsertInvestor$ } from "../../common/loader/investor";
import { DbInvestment, upsertInvestment$ } from "../../common/loader/investment";
import { addLatestBlockQuery$ } from "../../common/connector/block-query";
import { fetchBeefyPPFS$ } from "../connector/ppfs";
import { beefyBoostsFromGitHistory$ } from "../connector/boost-list";
import { fetchContractCreationInfos$ } from "../../common/connector/contract-creation";
import { DbImportStatus, fetchImportStatus$, upsertImportStatus$ } from "../../common/loader/import-status";
import { keyBy$ } from "../../../utils/rxjs/utils/key-by";

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
  // ---
  // from time to time, refresh vaults
  //
  // ---
  // create the insert pipeline for transfers
  // register logs for all contracts
  // emit to the pipeline on new log
  //

  const pollLiveData = withPgClient(async (client: PoolClient) => {
    const vaultPipeline$ = curry(importChainVaultTransfers)(client);
    const pipeline$ = productList$(client, "beefy").pipe(loaderByChain$(vaultPipeline$));
    return consumeObservable(pipeline$);
  });
  const pollBeefyProducts = withPgClient(loadBeefyProducts);
  const pollPriceData = withPgClient(fetchPrices);

  const backfillHistory = withPgClient(async (client: PoolClient) => {
    const vaultPipeline$ = curry(backfillChainHistory)(client);
    const pipeline$ = productList$(client, "beefy").pipe(loaderByChain$(vaultPipeline$));
    return consumeObservable(pipeline$);
  });

  return new Promise(async () => {
    // start polling live data immediately
    await pollBeefyProducts();
    //await backfillHistory();

    await pollLiveData();
    await pollPriceData();
    ////
    ////// then start polling at regular intervals
    //setInterval(pollLiveData, samplingPeriodMs["30s"]);
    ////setInterval(pollBeefyProducts, samplingPeriodMs["1day"]);
    //setInterval(pollPriceData, samplingPeriodMs["5min"]);
  });
}

runMain(main);

function backfillChainHistory(client: PoolClient, chain: Chain, beefyProduct$: Rx.Observable<DbBeefyProduct>) {
  const provider = new ethers.providers.JsonRpcBatchProvider(sample(RPC_URLS[chain]));
  const importPipeline$ = beefyProduct$.pipe(
    // find the import status and create it if needed

    // find the import status for these objects
    fetchImportStatus$({
      client: client,
      getProductId: (product) => product.productId,
      formatOutput: (product, importStatus) => ({ product, importStatus }),
    }),

    // extract those without an import status
    Rx.groupBy((item) => item.importStatus === null),
    Rx.map((isGroup$) => {
      // passthrough if we already have an import status
      if (isGroup$.key) {
        return isGroup$ as Rx.GroupedObservable<boolean, { product: DbBeefyProduct; importStatus: DbImportStatus }>;
      }

      // then for those whe can't find an import status
      return isGroup$.pipe(
        // find the contract creation block
        fetchContractCreationInfos$({
          provider: provider,
          getCallParams: (item) => ({
            chain: chain,
            contractAddress:
              item.product.productData.type === "beefy:vault"
                ? item.product.productData.vault.contract_address
                : item.product.productData.boost.contract_address,
          }),
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // create the import status
        upsertImportStatus$({
          client: client,
          getImportStatusData: (item) => ({
            productId: item.product.productId,
            importData: {
              type: "beefy",
              data: {
                contractCreatedAtBlock: item.contractCreationInfo.blockNumber,
                importedBlockRange: {
                  from: item.contractCreationInfo.blockNumber,
                  to: item.contractCreationInfo.blockNumber,
                },
                blockRangesToRetry: [],
              },
            },
          }),
          formatOutput: (item, importStatus) => ({ ...item, importStatus }),
        }),
      );
    }),
    // now all objects have an import status (and a contract creation block)
    Rx.mergeAll(),

    Rx.tap((item) => logger.debug({ msg: "import status", data: { importStatus: item.importStatus } })),
  );

  return importPipeline$;
}

function importChainVaultTransfers(
  client: PoolClient,
  chain: Chain,
  beefyProduct$: Rx.Observable<DbBeefyProduct>,
): Rx.Observable<DbInvestment> {
  const rpcUrl = sample(RPC_URLS[chain]) as string;
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);

  const [vaults$, boosts$] = Rx.partition(beefyProduct$, (p) => p.productData.type === "beefy:vault") as [
    Rx.Observable<DbBeefyVaultProduct>,
    Rx.Observable<DbBeefyBoostProduct>,
  ];

  const boostTransfers$ = boosts$.pipe(
    // keep active boosts only
    Rx.filter((product) => product.productData.boost.eol === false),

    // create an object we can safely add data to
    Rx.map((product) => ({ product })),

    // find out the blocks we want to query
    addLatestBlockQuery$({
      chain,
      provider,
      getLastImportedBlock: () => importState[chain].lastImportedBlockNumber ?? null,
      formatOutput: (item, latestBlocksQuery) => ({ ...item, latestBlocksQuery }),
    }),

    // fetch latest transfers from and to the boost contract
    fetchERC20TransferToAStakingContract$({
      provider,
      chain,
      getQueryParams: (item) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const boost = item.product.productData.boost;
        return {
          address: boost.staked_token_address,
          decimals: boost.staked_token_decimals,
          fromBlock: item.latestBlocksQuery.fromBlock,
          toBlock: item.latestBlocksQuery.toBlock,
          trackAddress: boost.contract_address,
        };
      },
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),
    handleRpcErrors$({ msg: "fetching boost transfers", data: { chain } }),

    // then we need the vault ppfs to interpret the balance
    fetchBeefyPPFS$({
      provider,
      chain,
      getPPFSCallParams: (item) => {
        const boost = item.product.productData.boost;
        return {
          vaultAddress: boost.staked_token_address,
          underlyingDecimals: item.product.productData.boost.vault_want_decimals,
          vaultDecimals: boost.staked_token_decimals,
          blockNumbers: item.transfers.map((t) => t.blockNumber),
        };
      },
      formatOutput: (item, ppfss) => ({ ...item, ppfss }),
    }),
    handleRpcErrors$({ msg: "fetching boosted vault ppfs", data: { chain } }),

    // add an ignore address so we can pipe this observable into the main pipeline again
    Rx.map((item) => ({ ...item, ignoreAddresses: [item.product.productData.boost.contract_address] })),
  );

  const allTransferQueries$ = vaults$
    // define the scope of our pipeline
    .pipe(
      //Rx.filter((vault) => vault.chain === "avax"), //debug

      Rx.filter((product) => product.productData.vault.eol === false), // only live vaults

      // create an object we can safely add data to
      Rx.map((product) => ({ product })),

      // find out the blocks we want to query
      addLatestBlockQuery$({
        chain,
        provider,
        getLastImportedBlock: () => importState[chain].lastImportedBlockNumber ?? null,
        formatOutput: (item, latestBlocksQuery) => ({ ...item, latestBlocksQuery }),
      }),
    );

  const [govTransferQueries$, transferQueries$] = Rx.partition(
    allTransferQueries$,
    ({ product }) => product.productData.vault.is_gov_vault,
  );

  const standardTransfersByVault$ = transferQueries$.pipe(
    // for standard vaults, we only ignore the mint-burn addresses
    Rx.map((item) => ({
      ...item,
      ignoreAddresses: [normalizeAddress("0x0000000000000000000000000000000000000000")],
    })),

    // fetch the vault transfers
    fetchErc20Transfers$({
      provider,
      chain,
      getQueryParams: (item) => {
        const vault = item.product.productData.vault;
        return {
          address: vault.contract_address,
          decimals: vault.token_decimals,
          fromBlock: item.latestBlocksQuery.fromBlock,
          toBlock: item.latestBlocksQuery.toBlock,
        };
      },
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    // we want to catch any errors from the RPC
    handleRpcErrors$({ msg: "mapping erc20Transfers", data: { chain } }),

    // fetch the ppfs
    fetchBeefyPPFS$({
      provider,
      chain,
      getPPFSCallParams: (item) => {
        return {
          vaultAddress: item.product.productData.vault.contract_address,
          underlyingDecimals: item.product.productData.vault.want_decimals,
          vaultDecimals: item.product.productData.vault.token_decimals,
          blockNumbers: item.transfers.map((t) => t.blockNumber),
        };
      },
      formatOutput: (item, ppfss) => ({ ...item, ppfss }),
    }),

    // we want to catch any errors from the RPC
    handleRpcErrors$({ msg: "mapping erc20Transfers", data: { chain } }),
  );

  const govTransfersByVault$ = govTransferQueries$.pipe(
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
      provider,
      chain,
      getQueryParams: (item) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const vault = item.product.productData.vault;
        return {
          address: vault.want_address,
          decimals: vault.want_decimals,
          fromBlock: item.latestBlocksQuery.fromBlock,
          toBlock: item.latestBlocksQuery.toBlock,
          trackAddress: vault.contract_address,
        };
      },
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    // we want to catch any errors from the RPC
    handleRpcErrors$({ msg: "mapping erc20Transfers", data: { chain } }),

    // simulate a ppfs of 1 so we can treat gov vaults like standard vaults
    Rx.map((item) => ({
      ...item,
      ppfss: item.transfers.map(() => new Decimal(1)),
    })),
  );

  // join gov and non gov vaults and boosts
  const rawTransfersByVault$ = Rx.merge(standardTransfersByVault$, govTransfersByVault$, boostTransfers$);

  const transfers$ = rawTransfersByVault$.pipe(
    // now move to transfers representation
    Rx.mergeMap((item) =>
      item.transfers.map((transfer, idx) => ({
        transfer,
        ignoreAddresses: item.ignoreAddresses,
        product: item.product,
        ppfs: item.ppfss[idx],
      })),
    ),

    // remove ignored addresses
    Rx.filter((item) => {
      const shouldIgnore = item.ignoreAddresses.some(
        (ignoreAddr) => ignoreAddr === normalizeAddress(item.transfer.ownerAddress),
      );
      if (shouldIgnore) {
        logger.debug({ msg: "ignoring transfer", data: { chain, transferData: item } });
      }
      return !shouldIgnore;
    }),

    Rx.tap((item) =>
      logger.trace({
        msg: "processing transfer data",
        data: {
          chain,
          productKey: item.product.productKey,
          transfer: item.transfer,
        },
      }),
    ),
  );

  const enhancedTransfers$ = transfers$.pipe(
    // we need the balance of each owner
    fetchERC20TokenBalance$({
      chain,
      provider,
      getQueryParams: (item) => ({
        blockNumber: item.transfer.blockNumber,
        decimals: item.transfer.tokenDecimals,
        contractAddress: item.transfer.tokenAddress,
        ownerAddress: item.transfer.ownerAddress,
      }),
      formatOutput: (item, vaultSharesBalance) => ({ ...item, vaultSharesBalance }),
    }),

    handleRpcErrors$({ msg: "mapping owner balance", data: { chain } }),

    // we also need the date of each block
    fetchBlockDatetime$({
      provider,
      getBlockNumber: (t) => t.transfer.blockNumber,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),

    // we want to catch any errors from the RPC
    handleRpcErrors$({ msg: "mapping block datetimes", data: { chain } }),
  );

  // insert relational data pipe
  const insertPipeline$ = enhancedTransfers$.pipe(
    // insert the investor data
    upsertInvestor$({
      client,
      getInvestorData: (item) => ({
        address: item.transfer.ownerAddress,
        investorData: {},
      }),
      formatOutput: (transferData, investorId) => ({ ...transferData, investorId }),
    }),

    upsertInvestment$({
      client,
      getInvestmentData: (item) => ({
        datetime: item.blockDatetime,
        productId: item.product.productId,
        investorId: item.investorId,
        // balance is expressed in underlying amount
        balance: item.vaultSharesBalance.mul(item.ppfs),
        investmentData: {
          blockNumber: item.transfer.blockNumber,
          balance: item.vaultSharesBalance.toString(),
          balanceDiff: item.transfer.amountTransfered.toString(),
          trxHash: item.transfer.transactionHash,
          ppfs: item.ppfs.toString(),
          productType:
            item.product.productData.type === "beefy:vault"
              ? item.product.productData.type + (item.product.productData.vault.is_gov_vault ? ":gov" : ":standard")
              : item.product.productData.type,
        },
      }),
      formatOutput: (_, investment) => investment,
    }),

    Rx.tap({ complete: () => logger.debug({ msg: "done processing chain", data: { chain } }) }),
  );

  return insertPipeline$;
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

      Rx.mergeMap((vaultsByChain$) =>
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
          Rx.concatMap(async (chainData) => {
            const chainBoosts =
              (await consumeObservable(
                beefyBoostsFromGitHistory$(chainData.chain, chainData.vaultByVaultId).pipe(Rx.toArray()),
              )) || [];

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
          }),
          Rx.mergeMap((boostsData) => Rx.from(boostsData)), // flatten

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
      // now we fetch
      Rx.concatMap(async (productData) => {
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
      }),

      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.mergeMap((productData) => Rx.from(productData.prices.map((price) => ({ ...productData, price })))),

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

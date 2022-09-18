import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { withPgClient } from "../../../utils/db";
import { beefyVaultsFromGitHistory$ } from "../connector/vault-list";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger";
import { consumeObservable } from "../../../utils/observable";
import { ethers } from "ethers";
import { curry, omit, sample } from "lodash";
import { RPC_URLS } from "../../../utils/config";
import { fetchErc20Transfers } from "../../common/connector/erc20-transfers";
import { TokenizedVaultUserTransfer } from "../../types/connector";
import { fetchERC20TokenBalance } from "../../common/connector/owner-balance";
import { fetchBlockDatetime } from "../../common/connector/block-datetime";
import { fetchBeefyPrices } from "../connector/prices";
import { loaderByChain } from "../../common/loader/loader-by-chain";
import Decimal from "decimal.js";
import { normalizeAddress } from "../../../utils/ethers";
import { handleRpcErrors } from "../../common/connector/rpc-errors";
import { fetchDbPriceFeed, upsertPriceFeed } from "../../common/loader/price-feed";
import {
  DbBeefyBoostProduct,
  DbBeefyProduct,
  DbBeefyVaultProduct,
  productList$,
  upsertProduct,
} from "../../common/loader/product";
import { normalizeVaultId } from "../utils/normalize-vault-id";
import { findMissingPriceRangeInDb, upsertPrices } from "../../common/loader/prices";
import { upsertInvestor } from "../../common/loader/investor";
import { DbInvestment, upsertInvestment } from "../../common/loader/investment";
import { addLatestBlockQuery } from "../../common/connector/latest-block-query";

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
    const pipeline$ = productList$(client, "beefy").pipe(loaderByChain(vaultPipeline$));
    return consumeObservable(pipeline$);
  });
  const pollBeefyProducts = withPgClient(loadBeefyProducts);
  const pollPriceData = withPgClient(fetchPrices);

  return new Promise(async () => {
    // start polling live data immediately
    //await pollBeefyProducts();
    await pollLiveData();
    await pollPriceData();

    // then start polling at regular intervals
    setInterval(pollLiveData, 1000 * 30 /* 30s */);
    //setInterval(pollVaultData, samplingPeriodMs["1day"]);
    setInterval(pollPriceData, 1000 * 60 * 5 /* 5 minutes */);
  });
}

runMain(main);

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

  const allTransferQueries$ = vaults$
    // define the scope of our pipeline
    .pipe(
      //Rx.filter((vault) => vault.chain === "avax"), //debug
      //Rx.filter((vault) => !vault.has_erc20_shares_token), // debug: gov vaults

      Rx.filter((product) => product.productData.vault.eol === false), // only live vaults

      // fetch the latest block numbers to query
      addLatestBlockQuery({
        chain,
        provider,
        getLastImportedBlock: (chain) => importState[chain].lastImportedBlockNumber ?? null,
        formatOutput: (product, latestBlocksQuery) => ({ product, latestBlocksQuery }),
      }),
    );

  const [govTransferQueries$, transferQueries$] = Rx.partition(
    allTransferQueries$,
    ({ product }) => product.productData.vault.is_gov_vault,
  );

  const standardTransfersByVault = transferQueries$.pipe(
    // for standard vaults, we only ignore the mint-burn addresses
    Rx.map((productQuery) => ({
      ...productQuery,
      ignoreAddresses: [normalizeAddress("0x0000000000000000000000000000000000000000")],
    })),

    // fetch the vault transfers
    fetchErc20Transfers({
      provider,
      chain,
      getQueryParams: (productQuery) => {
        const vault = productQuery.product.productData.vault;
        return {
          address: vault.contract_address,
          decimals: vault.token_decimals,
          fromBlock: productQuery.latestBlocksQuery.fromBlock,
          toBlock: productQuery.latestBlocksQuery.toBlock,
        };
      },
      formatOutput: (productQuery, transfers) => ({ ...productQuery, transfers }),
    }),

    // we want to catch any errors from the RPC
    handleRpcErrors({ msg: "mapping erc20Transfers", data: { chain } }),
  );

  const govTransfersByVault$ = govTransferQueries$.pipe(
    // for gov vaults, we ignore the vault address and the associated maxi vault to avoid double counting
    // todo: ignore the maxi vault
    Rx.map((productQuery) => ({
      ...productQuery,
      ignoreAddresses: [
        normalizeAddress("0x0000000000000000000000000000000000000000"),
        normalizeAddress(productQuery.product.productData.vault.contract_address),
      ],
    })),

    fetchErc20Transfers({
      provider,
      chain,
      getQueryParams: (productQuery) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const vault = productQuery.product.productData.vault;
        return {
          address: vault.want_address,
          decimals: vault.want_decimals,
          fromBlock: productQuery.latestBlocksQuery.fromBlock,
          toBlock: productQuery.latestBlocksQuery.toBlock,
          trackAddress: vault.contract_address,
        };
      },
      formatOutput: (productQuery, transfers) => ({ ...productQuery, transfers }),
    }),

    // we want to catch any errors from the RPC
    handleRpcErrors({ msg: "mapping erc20Transfers", data: { chain } }),

    // make is so we are transfering from the user in and out the vault
    Rx.map((productQuery) => ({
      ...productQuery,
      transfers: productQuery.transfers.map(
        (transfer): TokenizedVaultUserTransfer => ({
          ...transfer,
          vaultAddress: productQuery.product.productData.vault.contract_address,
          // amounts are reversed because we are sending token to the vault, but we then have a positive balance
          sharesBalanceDiff: transfer.sharesBalanceDiff.negated(),
        }),
      ),
    })),
  );

  // join gov and non gov vaults
  const rawTransfersByVault$ = Rx.merge(standardTransfersByVault, govTransfersByVault$);

  const transfers$ = rawTransfersByVault$.pipe(
    // now move to transfers representation
    Rx.mergeMap((productQuery) =>
      productQuery.transfers.map((transfer, idx) => ({
        transfer,
        ignoreAddresses: productQuery.ignoreAddresses,
        product: productQuery.product,
      })),
    ),

    // remove ignored addresses
    Rx.filter((transferData) => {
      const shouldIgnore = transferData.ignoreAddresses.some(
        (ignoreAddr) => ignoreAddr === normalizeAddress(transferData.transfer.ownerAddress),
      );
      if (shouldIgnore) {
        logger.debug({ msg: "ignoring transfer", data: { chain, transferData } });
      }
      return !shouldIgnore;
    }),

    Rx.tap((transferData) =>
      logger.trace({
        msg: "processing transfer data",
        data: {
          chain,
          productKey: transferData.product.productKey,
          transfer: transferData.transfer,
        },
      }),
    ),
  );

  const enhancedTransfers$ = transfers$.pipe(
    // we need the balance of each owner
    fetchERC20TokenBalance({
      chain,
      provider,
      getQueryParams: (transferData) => ({
        blockNumber: transferData.transfer.blockNumber,
        decimals: transferData.transfer.sharesDecimals,
        contractAddress: transferData.transfer.vaultAddress,
        ownerAddress: transferData.transfer.ownerAddress,
      }),
      formatOutput: (transferData, balance) => ({ ...transferData, balance }),
    }),

    handleRpcErrors({ msg: "mapping owner balance", data: { chain } }),

    // we also need the date of each block
    fetchBlockDatetime({
      provider,
      getBlockNumber: (t) => t.transfer.blockNumber,
      formatOutput: (transferData, blockDatetime) => ({ ...transferData, blockDatetime }),
    }),

    // we want to catch any errors from the RPC
    handleRpcErrors({ msg: "mapping block datetimes", data: { chain } }),
  );

  // insert relational data pipe
  const insertPipeline$ = enhancedTransfers$.pipe(
    // insert the investor data
    upsertInvestor({
      client,
      getInvestorData: (transferData) => ({
        investorAddress: transferData.transfer.ownerAddress,
        investorData: {},
      }),
      formatOutput: (transferData, investorId) => ({ ...transferData, investorId }),
    }),

    upsertInvestment({
      client,
      getInvestmentData: (transferData) => ({
        datetime: transferData.blockDatetime,
        productId: transferData.product.productId,
        investorId: transferData.investorId,
        balance: transferData.balance,
        investmentData: {
          blockNumber: transferData.transfer.blockNumber,
          balanceDiff: transferData.transfer.sharesBalanceDiff.toString(),
          trxHash: transferData.transfer.transactionHash,
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

  const vaultPipeline$ = Rx.of(...allChainIds).pipe(
    Rx.tap((chain) => logger.debug({ msg: "importing chain vaults", data: { chain } })),

    // fetch vaults from git file history
    Rx.mergeMap(
      beefyVaultsFromGitHistory$,
      1 /* concurrent = 1, we don't want concurrency here as we are doing local directory git work */,
    ),

    // create an object where we can add attributes to safely
    Rx.map((vault) => ({ vault })),

    // insert the product price key if needed
    upsertPriceFeed({
      client,
      getFeedData: (vaultData) => {
        const vaultId = normalizeVaultId(vaultData.vault.id);
        return {
          // the initial vault id is the price key
          feedKey: `beefy:vault:${vaultId}`,
          externalId: vaultId, // this is the key that the price feed uses
        };
      },
      formatOutput: (vaultData, assetPriceFeed) => ({ ...vaultData, assetPriceFeed }),
    }),

    upsertProduct({
      client,
      getProductData: (vaultData) => {
        const vaultId = normalizeVaultId(vaultData.vault.id);
        return {
          productKey: `beefy:vault:${vaultId}`,
          assetPriceFeedId: vaultData.assetPriceFeed.assetPriceFeedId,
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
      complete: () => {
        logger.info({ msg: "done vault configs data for all chains" });
      },
    }),
  );

  return consumeObservable(vaultPipeline$);
}

function fetchPrices(client: PoolClient) {
  logger.info({ msg: "fetching vault prices" });

  const [vaults$, boosts$] = Rx.partition(
    productList$(client, "beefy:vault"),
    (p) => p.productData.type === "beefy:vault",
  ) as [Rx.Observable<DbBeefyVaultProduct>, Rx.Observable<DbBeefyBoostProduct>];

  const pipeline$ = vaults$
    .pipe(
      Rx.tap((product) => logger.debug({ msg: "fetching vault price", data: product })),

      Rx.filter((product) => product.productData.vault.eol === false), // only live vaults
      // map to an object where we can add attributes to safely
      Rx.map((product) => ({ product })),

      // get the price feed infos
      fetchDbPriceFeed({
        client,
        getId: (productData) => productData.product.assetPriceFeedId,
        formatOutput: (productData, assetPriceFeed) => ({ ...productData, assetPriceFeed }),
      }),

      // remove duplicates
      Rx.distinct(({ assetPriceFeed }) => assetPriceFeed.externalId),

      // find which data is missing
      findMissingPriceRangeInDb({
        client,
        getFeedId: (productData) => productData.assetPriceFeed.assetPriceFeedId,
        formatOutput: (productData, missingData) => ({ ...productData, missingData }),
      }),
    )
    .pipe(
      // now we fetch
      Rx.mergeMap(async (productData) => {
        const debugLogData = {
          productKey: productData.product.productKey,
          priceFeed: productData.assetPriceFeed.assetPriceFeedId,
          missingData: productData.missingData,
        };

        logger.debug({ msg: "fetching prices", data: debugLogData });
        const prices = await fetchBeefyPrices("15min", productData.assetPriceFeed.externalId, {
          startDate: productData.missingData.fromDate,
          endDate: productData.missingData.toDate,
        });
        logger.debug({ msg: "got prices", data: { ...debugLogData, priceCount: prices.length } });

        return { ...productData, prices };
      }, 10 /* concurrency */),

      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.mergeMap((productData) => Rx.from(productData.prices.map((price) => ({ ...productData, price })))),

      upsertPrices({
        client,
        getPriceData: (priceData) => ({
          datetime: priceData.price.datetime,
          assetPriceFeedId: priceData.assetPriceFeed.assetPriceFeedId,
          usdValue: new Decimal(priceData.price.value),
        }),
        formatOutput: (priceData, price) => ({ ...priceData, price }),
      }),
    );

  return consumeObservable(pipeline$).then(() => logger.info({ msg: "done fetching prices" }));
}

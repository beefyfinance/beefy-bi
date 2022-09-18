import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { withPgClient } from "../../../utils/db";
import { beefyVaultsFromGitHistory$ } from "../connector/vault-list";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger2";
import { consumeObservable } from "../../../utils/observable";
import { ethers } from "ethers";
import { curry, keyBy, max, sample, sortBy } from "lodash";
import { samplingPeriodMs } from "../../../types/sampling";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, MS_PER_BLOCK_ESTIMATE, RPC_URLS } from "../../../utils/config";
import { mapErc20Transfers } from "../../common/connector/erc20-transfers";
import { TokenizedVaultUserTransfer } from "../../types/connector";
import { mapBeefyVaultShareRate } from "../connector/ppfs";
import { mapERC20TokenBalance } from "../../common/connector/owner-balance";
import { mapBlockDatetime } from "../../common/connector/block-datetime";
import { fetchBeefyPrices } from "../connector/prices";
import { loaderByChain } from "../../common/loader/loader-by-chain";
import Decimal from "decimal.js";
import { normalizeAddress } from "../../../utils/ethers";
import { handleRpcErrors } from "../../common/connector/rpc-errors";
import { mapPriceFeed, upsertPriceFeed } from "../../common/loader/price-feed";
import {
  DbBeefyBoostProduct,
  DbBeefyProduct,
  DbBeefyVaultProduct,
  productList$,
  upsertProduct,
} from "../../common/loader/product";
import { normalizeVaultId } from "../utils/normalize-vault-id";
import { toMissingPriceDataQuery, upsertPrices } from "../../common/loader/prices";
import { upsertInvestor } from "../../common/loader/investor";
import { DbInvestment, upsertInvestment } from "../../common/loader/investment";

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
    //await pollPriceData();

    // then start polling at regular intervals
    //setInterval(pollLiveData, 1000 * 30 /* 30s */);
    //setInterval(pollVaultData, samplingPeriodMs["1day"]);
    //setInterval(pollPriceData, 1000 * 60 * 5 /* 5 minutes */);
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
    )
    .pipe(
      // batch vault config by some reasonable amount that the RPC can handle
      Rx.bufferCount(200),
      // go get the latest block number for this chain
      Rx.mergeMap(async (products) => {
        const latestBlockNumber = await provider.getBlockNumber();
        return products.map((product) => ({ product, latestBlockNumber }));
      }),

      // compute the block range we want to query
      Rx.map((productQueries) =>
        productQueries.map((productQuery) => {
          // fetch the last hour of data
          const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[chain];
          const period = samplingPeriodMs["1hour"];
          const periodInBlockCountEstimate = Math.floor(period / MS_PER_BLOCK_ESTIMATE[chain]);

          const lastImportedBlockNumber = importState[chain].lastImportedBlockNumber ?? null;
          const diffBetweenLastImported = lastImportedBlockNumber
            ? productQuery.latestBlockNumber - (lastImportedBlockNumber + 1)
            : Infinity;

          const blockCountToFetch = Math.min(maxBlocksPerQuery, periodInBlockCountEstimate, diffBetweenLastImported);
          const fromBlock = productQuery.latestBlockNumber - blockCountToFetch;
          const toBlock = productQuery.latestBlockNumber;

          // also wait some time to avoid errors like "cannot query with height in the future; please provide a valid height: invalid height"
          // where the RPC don't know about the block number he just gave us
          const waitForBlockPropagation = 5;
          return {
            ...productQuery,
            fromBlock: fromBlock - waitForBlockPropagation,
            toBlock: toBlock - waitForBlockPropagation,
          };
        }),
      ),
    );

  const [govTransferQueries$, transferQueries$] = Rx.partition(
    allTransferQueries$.pipe(Rx.mergeMap((products) => Rx.from(products))),
    ({ product }) => product.productData.vault.is_gov_vault,
  );

  const standardTransfersByVault = transferQueries$.pipe(
    // for standard vaults, we only ignore the mint-burn addresses
    Rx.map((productQuery) => ({
      ...productQuery,
      ignoreAddresses: [normalizeAddress("0x0000000000000000000000000000000000000000")],
    })),

    // batch vault config by some reasonable amount that the RPC can handle
    Rx.bufferCount(200),

    mapErc20Transfers(
      provider,
      chain,
      (productQuery) => {
        const vault = productQuery.product.productData.vault;
        return {
          address: vault.contract_address,
          decimals: vault.token_decimals,
          fromBlock: productQuery.fromBlock,
          toBlock: productQuery.toBlock,
        };
      },
      "transfers",
    ),

    // we want to catch any errors from the RPC
    handleRpcErrors({ msg: "mapping erc20Transfers", data: { chain } }),

    // we also want the shares rate of each involved vault
    mapBeefyVaultShareRate(
      provider,
      chain,
      (productQuery) => {
        const vault = productQuery.product.productData.vault;
        return {
          vaultAddress: vault.contract_address,
          vaultDecimals: vault.token_decimals,
          underlyingDecimals: vault.want_decimals,
          blockNumbers: productQuery.transfers.map((t) => t.blockNumber),
        };
      },
      "shareToUnderlyingRates",
    ),

    handleRpcErrors({ msg: "mapping vault share rate", data: { chain } }),

    // flatten the result
    Rx.mergeMap((vaults) => Rx.from(vaults)),
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

    // batch vault config by some reasonable amount that the RPC can handle
    Rx.bufferCount(200),

    mapErc20Transfers(
      provider,
      chain,
      (productQuery) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const vault = productQuery.product.productData.vault;
        return {
          address: vault.want_address,
          decimals: vault.want_decimals,
          fromBlock: productQuery.fromBlock,
          toBlock: productQuery.toBlock,
          trackAddress: vault.contract_address,
        };
      },
      "transfers",
    ),

    // we want to catch any errors from the RPC
    handleRpcErrors({ msg: "mapping erc20Transfers", data: { chain } }),

    // flatten
    Rx.mergeMap((productQueries) => Rx.from(productQueries)),

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

    // we set the share rate to 1 for gov vaults
    Rx.map((vault) => ({ ...vault, shareToUnderlyingRates: vault.transfers.map(() => new Decimal(1)) })),
  );

  // join gov and non gov vaults
  const rawTransfersByVault$ = Rx.merge(standardTransfersByVault, govTransfersByVault$);

  const transfers$ = rawTransfersByVault$.pipe(
    // now move to transfers representation
    Rx.mergeMap((productQuery) =>
      productQuery.transfers.map((transfer, idx) => ({
        ...transfer,
        shareToUnderlyingRate: productQuery.shareToUnderlyingRates[idx],
        ignoreAddresses: productQuery.ignoreAddresses,
        product: productQuery.product,
      })),
    ),

    // remove ignored addresses
    Rx.filter((transfer) => {
      const shouldIgnore = transfer.ignoreAddresses.some(
        (ignoreAddr) => ignoreAddr === normalizeAddress(transfer.ownerAddress),
      );
      if (shouldIgnore) {
        logger.debug({ msg: "ignoring transfer", data: { chain, transfer } });
      }
      return !shouldIgnore;
    }),

    Rx.tap((userAction) =>
      logger.trace({
        msg: "processing user action",
        data: {
          chain,
          productKey: userAction.product.productKey,
          transaction: userAction.transactionHash,
          ownerAddress: userAction.ownerAddress,
          vaultAddress: userAction.vaultAddress,
          amount: userAction.sharesBalanceDiff.toString(),
        },
      }),
    ),
  );

  const enhancedTransfers$ = transfers$.pipe(
    // batch transfer events before fetching additional infos
    Rx.bufferCount(500),

    // we need the balance of each owner
    mapERC20TokenBalance(
      chain,
      provider,
      (t) => ({
        blockNumber: t.blockNumber,
        decimals: t.sharesDecimals,
        contractAddress: t.vaultAddress,
        ownerAddress: t.ownerAddress,
      }),
      "ownerBalance",
    ),

    handleRpcErrors({ msg: "mapping owner balance", data: { chain } }),

    // we also need the date of each block
    mapBlockDatetime(provider, (t) => t.blockNumber, "blockDatetime"),

    // we want to catch any errors from the RPC
    handleRpcErrors({ msg: "mapping block datetimes", data: { chain } }),

    // flatten the resulting array
    Rx.mergeMap((transfers) => Rx.from(transfers)),
  );

  // insert relational data pipe
  const insertPipeline$ = enhancedTransfers$.pipe(
    // insert the investor data
    upsertInvestor(
      client,
      (transfer) => ({
        investorAddress: transfer.ownerAddress,
        investorData: {},
      }),
      "investorId",
    ),

    // prepare data for insert
    Rx.map((transfer) => ({
      datetime: transfer.blockDatetime,
      productId: transfer.product.productId,
      investorId: transfer.investorId,
      balance: transfer.ownerBalance,
      investmentData: {
        blockNumber: transfer.blockNumber,
        ppfs: transfer.shareToUnderlyingRate.toString(),
        balanceDiff: transfer.sharesBalanceDiff.toString(),
        trxHash: transfer.transactionHash,
      },
    })),

    upsertInvestment(client),

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

    // map to product input representation
    Rx.map((vault) => {
      const vaultId = normalizeVaultId(vault.id);

      return {
        productKey: `beefy:vault:${vaultId}`,

        assetPriceFeed: {
          // the initial vault id is the price key
          feedKey: `beefy:vault:${vaultId}`,
          externalId: vaultId, // this is the key that the price feed uses
        },

        chain: vault.chain,

        productData: {
          type: "beefy:vault",
          vault,
        },
      };
    }),

    // insert the product price key if needed
    upsertPriceFeed(client),

    // prepare for product insert
    Rx.map((product) => ({ product })),

    upsertProduct(client),

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

      // get the price feed infos
      mapPriceFeed(client, (product) => product.assetPriceFeedId, "assetPriceFeed"),

      // remove duplicates
      Rx.distinct((product) => product.assetPriceFeed.externalId),
    )
    .pipe(toMissingPriceDataQuery(client))
    .pipe(
      // now we fetch
      Rx.mergeMap(async (priceFeedQuery) => {
        logger.debug({ msg: "fetching prices", data: priceFeedQuery });
        const prices = await fetchBeefyPrices("15min", priceFeedQuery.obj.assetPriceFeed.externalId, {
          startDate: priceFeedQuery.fromDate,
          endDate: priceFeedQuery.toDate,
        });
        logger.debug({ msg: "got prices", data: { priceFeedQuery, prices: prices.length } });
        return prices.map((p) => ({
          datetime: p.datetime,
          assetPriceFeedId: priceFeedQuery.obj.assetPriceFeedId,
          usdValue: new Decimal(p.value),
        }));
      }, 10 /* concurrency */),

      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.mergeMap((prices) => Rx.from(prices)),

      upsertPrices(client),
    );

  return consumeObservable(pipeline$).then(() => logger.info({ msg: "done fetching prices" }));
}

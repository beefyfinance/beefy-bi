import { keyBy, uniqBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { DbClient, db_query, strAddressToPgBytea } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { upsertIgnoreAddress$ } from "../../common/loader/ignore-address";
import { productList$ } from "../../common/loader/product";
import { ErrorEmitter, ImportCtx } from "../../common/types/import-context";
import { dbBatchCall$ } from "../../common/utils/db-batch";
import { createChainRunner, NoRpcRunnerConfig } from "../../common/utils/rpc-chain-runner";
import { beefyZapsFromGit$ } from "../connector/zap-list";
import { getProductContractAddress } from "../utils/contract-accessors";
import { isBeefyGovVault, isBeefyStandardVault } from "../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "import-products" });

export function createBeefyIgnoreAddressRunner(options: { client: DbClient; runnerConfig: NoRpcRunnerConfig<Chain> }) {
  const createPipeline = (ctx: ImportCtx) => {
    const zapAddresses$ = Rx.pipe(
      // fetch vaults from git file history
      Rx.concatMap((chain: Chain) => {
        logger.info({ msg: "importing beefy ignore addresses", data: { chain } });
        return beefyZapsFromGit$().pipe(
          // only keep those for the current chain
          Rx.filter((zap) => zap.chain === chain),
        );
      }),
      Rx.map((zap) => ({
        address: zap.address,
        chain: zap.chain,
        restrictToProductId: null,
      })),
    );

    // we never want to track beefy product addresses as they are not real investors
    const beefyProducts$ = Rx.pipe(
      Rx.mergeMap((chain: Chain) => productList$(options.client, "beefy", chain)),

      Rx.map((product) => ({
        address: getProductContractAddress(product),
        chain: product.chain,
        restrictToProductId: null,
      })),
    );

    // also ignore the maxi vault from gov vaults
    const maxiVaults$ = Rx.pipe(
      Rx.mergeMap((chain: Chain) =>
        productList$(options.client, "beefy", chain).pipe(
          Rx.toArray(),
          Rx.mergeMap((products) => {
            const maxiVault = products.filter(isBeefyStandardVault).find((product) => product.productData.vault.id.endsWith("bifi-maxi"));
            const govVault = products.filter(isBeefyGovVault).find((product) => product.productData.vault.id.endsWith("bifi-gov"));

            if (maxiVault && govVault) {
              return Rx.from([
                {
                  address: getProductContractAddress(govVault),
                  chain: govVault.chain,
                  restrictToProductId: maxiVault.productId,
                },
              ]);
            } else {
              return Rx.EMPTY;
            }
          }),
        ),
      ),
    );

    // also ignore the default mintburn address
    const defaultMintBurnAddress$ = Rx.pipe(
      Rx.map((chain: Chain) => ({
        address: "0x0000000000000000000000000000000000000000",
        chain,
        restrictToProductId: null,
      })),
    );

    return Rx.pipe(
      Rx.connect((chains$: Rx.Observable<Chain>) =>
        Rx.merge(chains$.pipe(beefyProducts$), chains$.pipe(maxiVaults$), chains$.pipe(defaultMintBurnAddress$), chains$.pipe(zapAddresses$)),
      ),

      // insert the zap list as an ignored address
      upsertIgnoreAddress$({
        ctx,
        emitError: (err) => logger.error({ msg: "error importing ignore address", data: { err } }),
        getIgnoreAddressData: (data) => data,
        formatOutput: (product) => product,
      }),

      Rx.pipe(
        Rx.tap({
          error: (err) => logger.error({ msg: "error importing ignore address", data: { err } }),
          complete: () => {
            logger.info({ msg: "done importing ignore addresses" });
          },
        }),
      ),
    );
  };

  return createChainRunner(options.runnerConfig, createPipeline);
}

export function shouldIgnoreAddress$<
  TObj,
  TErr extends ErrorEmitter<TObj>,
  TRes,
  TParams extends { transferAddress: string; productId: number },
>(options: { ctx: ImportCtx; emitError: TErr; getAddressData: (obj: TObj) => TParams; formatOutput: (obj: TObj, shouldIgnore: boolean) => TRes }) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    getData: options.getAddressData,
    logInfos: { msg: "checking if address should be ignored", data: { chain: options.ctx.chain } },
    processBatch: async (objAndData) => {
      const result = await db_query<{ address: string; restrict_to_product_id: number | null }>(
        `
        SELECT lower(bytea_to_hexstr(address)) as address, restrict_to_product_id
        FROM ignore_address
        WHERE chain = %L
        AND address IN (%L)
      `,
        [
          options.ctx.chain,
          uniqBy(objAndData, ({ data }) => data.transferAddress).map(({ data }) => [strAddressToPgBytea(data.transferAddress), data.productId]),
          options.ctx.chain,
        ],
        options.ctx.client,
      );

      const ignoreAddressMap = keyBy(result, (row) => row.address);
      return new Map(
        objAndData.map(({ data }) => {
          const ignoreAddress = ignoreAddressMap[data.transferAddress.toLocaleLowerCase()];
          if (!ignoreAddress) return [data, false];
          if (ignoreAddress.restrict_to_product_id === null) return [data, true];
          if (ignoreAddress.restrict_to_product_id === data.productId) return [data, true];
          return [data, false];
        }),
      );
    },
    formatOutput: options.formatOutput,
  });
}

export async function createShouldIgnoreFn(options: {
  client: DbClient;
  chain: Chain;
}): Promise<(ownerAddress: string, productId: number) => boolean> {
  const result = await db_query<{ address: string; restrict_to_product_id: number | null }>(
    `
    SELECT lower(bytea_to_hexstr(address)) as address, restrict_to_product_id
    FROM ignore_address
    WHERE chain = %L
  `,
    [options.chain],
    options.client,
  );

  const ignoreAddressMap = keyBy(result, (row) => row.address);
  return (ownerAddress, productId) => {
    const ignoreAddress = ignoreAddressMap[ownerAddress.toLocaleLowerCase()];
    if (!ignoreAddress) return false;
    if (ignoreAddress.restrict_to_product_id === null) return true;
    if (ignoreAddress.restrict_to_product_id === productId) return true;
    return false;
  };
}

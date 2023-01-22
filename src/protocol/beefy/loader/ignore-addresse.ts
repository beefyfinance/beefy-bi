import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { DbClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { upsertIgnoreAddress$ } from "../../common/loader/ignore-address";
import { productList$ } from "../../common/loader/product";
import { ImportCtx } from "../../common/types/import-context";
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

            console.log({ maxiVault, govVault });

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

    return Rx.pipe(
      Rx.connect((chains$: Rx.Observable<Chain>) => Rx.merge(chains$.pipe(zapAddresses$), chains$.pipe(beefyProducts$), chains$.pipe(maxiVaults$))),

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

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

      // insert the zap list as an ignroed address
      upsertIgnoreAddress$({
        ctx,
        emitError: (err) => logger.error({ msg: "error importing ignore address", data: { err } }),
        getIgnoreAddressData: (zap) => ({
          address: zap.address,
          chain: zap.chain,
          restrictToProductId: null,
        }),
        formatOutput: (zap) => zap,
      }),

      // add zap addresses to ignore list
      Rx.pipe(
        Rx.tap({
          error: (err) => logger.error({ msg: "error importing ignore address", data: { err } }),
          complete: () => {
            logger.info({ msg: "done importing ignore addresses" });
          },
        }),
      ),
    );

    // we never want to track beefy product addresses as they are not real investors
    const beefyProducts$ = Rx.pipe(
      Rx.mergeMap((chain: Chain) => productList$(options.client, "beefy", chain)),

      // insert the zap list as an ignroed address
      upsertIgnoreAddress$({
        ctx,
        emitError: (err) => logger.error({ msg: "error importing ignore address", data: { err } }),
        getIgnoreAddressData: (product) => {
          return {
            address: getProductContractAddress(product),
            chain: product.chain,
            restrictToProductId: null,
          };
        },
        formatOutput: (product) => product,
      }),
    );

    // also ignore the maxi vault from gov vaults
    const maxiVaults$ = Rx.pipe(
      Rx.mergeMap((chain: Chain) => productList$(options.client, "beefy", chain)),
      Rx.toArray(),
      Rx.map((products) => {
        const maxiVault = products.find((product) => product.productKey.endsWith("bifi-maxi"));
        const govVault = products.find((product) => product.productKey.endsWith("bifi-gov"));
        return maxiVault;
      }),
    );

    return Rx.connect((chains$: Rx.Observable<Chain>) =>
      Rx.merge(chains$.pipe(zapAddresses$), chains$.pipe(beefyProducts$), chains$.pipe(maxiVaults$)),
    );
  };

  return createChainRunner(options.runnerConfig, createPipeline);
}

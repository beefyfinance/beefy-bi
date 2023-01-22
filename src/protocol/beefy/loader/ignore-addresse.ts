import { keyBy, uniqBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { DbClient, db_query, strAddressToPgBytea } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { DbIgnoreAddress, upsertIgnoreAddress$ } from "../../common/loader/ignore-address";
import { productList$ } from "../../common/loader/product";
import { ErrorEmitter, ImportCtx } from "../../common/types/import-context";
import { dbBatchCall$ } from "../../common/utils/db-batch";
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
      Rx.map((zap) => ({
        address: zap.address,
        chain: zap.chain,
      })),
    );

    // we never want to track beefy product addresses as they are not real investors
    const beefyProducts$ = Rx.pipe(
      Rx.mergeMap((chain: Chain) => productList$(options.client, "beefy", chain)),

      Rx.map((product) => ({
        address: getProductContractAddress(product),
        chain: product.chain,
      })),
    );

    const manualAddresses$ = Rx.pipe(
      Rx.mergeMap((chain: Chain) => [
        // also ignore the default mintburn address
        {
          address: "0x0000000000000000000000000000000000000000",
          chain,
        },
        // a GateManagerMultiRewards contract
        {
          address: "0x2c85A6aA1974230d3e91bA32956271E5A11b0392",
          chain: "bsc",
        },
        {
          address: "0xB9208616368c362c531aFE3b813a5cef181399b7",
          chain: "bsc",
        },
        // a MooTokenYieldSource contract
        {
          address: "0x27CAa27B47182873AdD2ACFf8212a5A664C1a4a0",
          chain: "bsc",
        },
        {
          address: "0x155F5AcBda6ad32e74297836139edC13ee391a35",
          chain: "bsc",
        },
        // crosschainQiStablecoin
        {
          address: "0x75D4aB6843593C111Eeb02Ff07055009c836A1EF",
          chain: "fantom",
        },
        {
          address: "0x3609A304c6A41d87E895b9c1fd18c02ba989Ba90",
          chain: "fantom",
        },
        {
          address: "0xBf0ff8ac03f3E0DD7d8faA9b571ebA999a854146",
          chain: "fantom",
        },
        {
          address: "0x5563Cc1ee23c4b17C861418cFF16641D46E12436",
          chain: "fantom",
        },
        {
          address: "0xC1c7eF18ABC94013F6c58C6CdF9e829A48075b4e",
          chain: "fantom",
        },
        {
          address: "0x8e5e4D08485673770Ab372c05f95081BE0636Fa2",
          chain: "fantom",
        },
        // a bethoven X vault
        {
          address: "0x20dd72Ed959b6147912C2e529F0a0C651c33c9ce",
          chain: "fantom",
        },
        // TOMB CErc20Delegator
        {
          address: "0x3F4f523ACf811E713e7c34852b24E927D773a9e5",
          chain: "fantom",
        },
        // StrategybeTokenRewardPool
        {
          address: "0xBC3Df9A5879432135f4A976795b1fC1728279012",
          chain: "polygon",
        },
        // an old BeefyZapOneInch ?
        {
          address: "0x3983C50fF4CD25b43A335D63839B1E36C7930D41",
          chain: "optimism",
        }
      ] satisfies DbIgnoreAddress[]),
    );

    return Rx.pipe(
      Rx.connect((chains$: Rx.Observable<Chain>) =>
        Rx.merge(chains$.pipe(beefyProducts$), chains$.pipe(manualAddresses$), chains$.pipe(zapAddresses$)),
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

export async function createShouldIgnoreFn(options: {
  client: DbClient;
  chain: Chain;
}): Promise<(ownerAddress: string) => boolean> {
  const result = await db_query<{ address: string; }>(
    `
    SELECT lower(bytea_to_hexstr(address)) as address
    FROM ignore_address
    WHERE chain = %L
  `,
    [options.chain],
    options.client,
  );

  const ignoreAddressMap = keyBy(result, (row) => row.address);
  return (ownerAddress) => {
    const ignoreAddress = ignoreAddressMap[ownerAddress.toLocaleLowerCase()];
    if (!ignoreAddress) return false;
    return false;
  };
}

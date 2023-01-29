import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { DbClient, db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { DbIgnoreAddress, upsertIgnoreAddress$ } from "../../common/loader/ignore-address";
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
      Rx.mergeMap((chain: Chain): DbIgnoreAddress[] => [
        // also ignore the default mintburn address
        { address: "0x0000000000000000000000000000000000000000", chain },
        { address: "0x000000000000000000000000000000000000dead", chain: "fantom" },
        // a GateManagerMultiRewards contract
        { address: "0x2c85A6aA1974230d3e91bA32956271E5A11b0392", chain: "bsc" },
        { address: "0xB9208616368c362c531aFE3b813a5cef181399b7", chain: "bsc" },
        { address: "0x00957197874Ee40211Fe15b205754124834da532", chain: "fantom" },
        // a MooTokenYieldSource contract
        { address: "0x27CAa27B47182873AdD2ACFf8212a5A664C1a4a0", chain: "bsc" },
        { address: "0x155F5AcBda6ad32e74297836139edC13ee391a35", chain: "bsc" },
        { address: "0x4F44b32Be113e56f279554eFa4e70128Dc57C7eC", chain: "fantom" },
        // crosschainQiStablecoin
        { address: "0x75D4aB6843593C111Eeb02Ff07055009c836A1EF", chain: "fantom" },
        { address: "0x3609A304c6A41d87E895b9c1fd18c02ba989Ba90", chain: "fantom" },
        { address: "0xBf0ff8ac03f3E0DD7d8faA9b571ebA999a854146", chain: "fantom" },
        { address: "0x5563Cc1ee23c4b17C861418cFF16641D46E12436", chain: "fantom" },
        { address: "0xC1c7eF18ABC94013F6c58C6CdF9e829A48075b4e", chain: "fantom" },
        { address: "0x8e5e4D08485673770Ab372c05f95081BE0636Fa2", chain: "fantom" },
        // ProfitDistribution
        { address: "0x8bf3552f84FF21056d92F5E7BC47EE613786D4fC", chain: "fantom" },
        // stableQiVault
        { address: "0xF9CE2522027bD40D3b1aEe4abe969831FE3BeAf5", chain: "optimism" },
        // a bethoven X vault
        { address: "0x20dd72Ed959b6147912C2e529F0a0C651c33c9ce", chain: "fantom" },
        // TOMB CErc20Delegator
        { address: "0x3F4f523ACf811E713e7c34852b24E927D773a9e5", chain: "fantom" },
        // StrategybeTokenRewardPool
        { address: "0xBC3Df9A5879432135f4A976795b1fC1728279012", chain: "polygon" },
        // an old BeefyZapOneInch ?
        { address: "0x3983C50fF4CD25b43A335D63839B1E36C7930D41", chain: "optimism" },
        // bifi-maxi strategies
        { address: "0x230691a28C8290A553BFBC911Ab2AbA0b2df152D", chain: "fantom" },
        { address: "0xD126BA764D2fA052Fc14Ae012Aef590Bc6aE0C4f", chain: "polygon" },
        { address: "0x24AAaB9DA14308bAf9d670e2a37369FE8Cb5Fe36", chain: "fuse" },
        { address: "0xa9E6E271b27b20F65394914f8784B3B860dBd259", chain: "cronos" },
        { address: "0xca077eEC87e2621F5B09AFE47C42BAF88c6Af18c", chain: "avax" },
        { address: "0xC808b28A006c91523De75EA23F48BE8b7a9536D1", chain: "optimism" },
        { address: "0xc9a509dA14525Ad3710e9448a0839EE2e90E48B1", chain: "moonriver" },
        // beefyZapper
        { address: "0xE2379CB4c4627E5e9dF459Ce08c2342C696C4c1f", chain: "fantom" },
        // BeefyVaultV6Passthrough
        { address: "0xc2D2A8E5237D17AbD9313dc17b5Ab2A9b8c8C019", chain: "fantom" },
      ]),
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

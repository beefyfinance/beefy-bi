import { keyBy } from "lodash";
import * as path from "path";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { GITHUB_RO_AUTH_TOKEN, GIT_WORK_DIRECTORY } from "../../../utils/config";
import { normalizeAddressOrThrow } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { GitFileVersion, gitStreamFileVersions } from "../../common/connector/git-file-history";
import { normalizeVaultId } from "../utils/normalize-vault-id";
import { BeefyVault } from "./vault-list";

const logger = rootLogger.child({ module: "beefy", component: "boost-list" });

 type RawTokenRewardConfig = {
  type: 'token';
  address: string;
  symbol: string;
  decimals: number;
  oracleId: string;
  oracle?: 'lps' | 'tokens';
  chainId?: string;
};

/*type RawPointsRewardConfig = {
  type: 'points';
  name: string;
};*/

export type RawRewardConfig = RawTokenRewardConfig /*| RawPointsRewardConfig*/;

type RawBeefyBoost = {
  id: string;
  vaultId: string;
  title: string;
  status: 'active' | 'prestake' | 'inactive';

  type: 'boost';
  contractAddress: string;
  version?: number;
  rewards: RawRewardConfig[];
}

type RawBeefyPromo = RawBeefyBoost | {type: 'offchain'} | {type: 'pool'} | {type: 'airdrop'};

export interface BeefyBoost {
  id: string;
  chain: Chain;

  vault_id: string;
  name: string;
  contract_address: string;

  // end of life
  eol: boolean;
  eol_date: Date | null;

  staked_token_address: string;
  staked_token_decimals: number;
  vault_want_address: string;
  vault_want_decimals: number;

  reward_token_symbol: string;
  reward_token_decimals: number;
  reward_token_address: string;
  reward_token_price_feed_key: string;
}

export function beefyBoostsFromGitHistory$(chain: Chain, allChainVaults: BeefyVault[]): Rx.Observable<BeefyBoost> {
  logger.debug({ msg: "Fetching boost list from beefy-v2 repo git history", data: { chain } });

  const fileContentStreamV2 = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-v2.git`
      : "https://github.com/beefyfinance/beefy-v2.git",
    branch: "main",
    filePath: `src/config/promos/chain/${chain}.json`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v2"),
    order: "old-to-recent",
    throwOnError: false,
    onePerMonth: false,
  });

  const v2$ = Rx.from(fileContentStreamV2).pipe(
    // parse the file content
    Rx.map((fileVersion) => {
      try {
        const boosts = JSON.parse(fileVersion.fileContent) as RawBeefyPromo[];
        return { fileVersion, boosts };
      } catch (error) {
        logger.error({ msg: "Could not parse boost file", data: { fileVersion }, error });
        logger.debug(error);
      }
      return { fileVersion, boosts: [] as RawBeefyPromo[] };
    }),
  );

  const vaultMap = keyBy(allChainVaults, (vault) => normalizeVaultId(vault.id));

  // process in chronological order
  return v2$.pipe(
    // process the vaults in chronolical order and mark the eol date if found
    Rx.reduce((acc, { fileVersion, boosts }) => {
      try {
        // reset the foundInCurrentBatch flag
        for (const boostAddress of Object.keys(acc)) {
          acc[boostAddress].foundInCurrentBatch = false;
        }

        // add boosts to the accumulator
        for (const boost of boosts) {
          if(boost.type !== 'boost') continue;

          const boostAddress = normalizeAddressOrThrow(boost.contractAddress);
          if (!acc[boostAddress]) {
            const eolDate = boost.status === "inactive" ? fileVersion.date : null;
            acc[boostAddress] = { fileVersion, eolDate, boost, foundInCurrentBatch: true };
          } else {
            const eolDate = boost.status === "inactive" ? acc[boostAddress].eolDate || fileVersion.date : null;
            acc[boostAddress] = { boost, eolDate, foundInCurrentBatch: true, fileVersion };
          }
        }

        // mark all deleted vaults as eol if not already done
        for (const boostAddress of Object.keys(acc)) {
          if (!acc[boostAddress].foundInCurrentBatch) {
            acc[boostAddress].boost.status = "inactive";
            acc[boostAddress].eolDate = acc[boostAddress].eolDate || fileVersion.date;
          }
        }

        return acc;
      } catch (error) {
        logger.error({ msg: "Could not process boost file", data: { fileVersion, boosts }, error });
        logger.debug(error);
        return acc;
      }
    }, {} as Record<string, { foundInCurrentBatch: boolean; fileVersion: GitFileVersion; eolDate: Date | null; boost: RawBeefyBoost }>),

    // flatten the accumulator
    Rx.map((acc) => Object.values(acc)),
    Rx.concatAll(),

    Rx.tap(({ fileVersion, boost, eolDate }) =>
      logger.trace({
        msg: "Boost from git history",
        data: { fileVersion: { ...fileVersion, fileContent: "<removed>" }, boost, isEol: boost.status === "inactive", eolDate },
      }),
    ),

    Rx.tap(({ fileVersion, boost, eolDate }) => {
      if (boost.status === "inactive" && !eolDate) {
        logger.error({
          msg: "product marked as eol but no eol date found",
          data: { fileVersion: { ...fileVersion, fileContent: "<removed>" }, boost, eolDate },
        });
      }
    }),

    // just emit the boost
    Rx.concatMap(({ boost, eolDate }) => {
      const vault = vaultMap[normalizeVaultId(boost.vaultId)];
      if (!vault) {
        logger.error({ msg: "Could not find vault for boost", data: { boostId: boost.id, vaultId: normalizeVaultId(boost.vaultId) } });
        return Rx.EMPTY;
      }

      try {
        return Rx.of(rawBoostToBeefyBoost(chain, boost, vault, eolDate));
      } catch (error) {
        logger.error({ msg: "Could not map raw boost to expected format", data: { rawVault: boost }, error });
        logger.debug(error);
        return Rx.EMPTY;
      }
    }),

    Rx.tap({
      complete: () => logger.debug({ msg: "Finished fetching boost list from beefy-v2 repo git history", data: { chain } }),
    }),
  );
}

function rawBoostToBeefyBoost(chain: Chain, rawBoost: RawBeefyBoost, vault: BeefyVault, eolDate: Date | null): BeefyBoost {
  try {
    const rewards = rawBoost.rewards.filter((reward): reward is RawTokenRewardConfig => reward.type === "token");
    return {
      id: rawBoost.id,
      chain,

      vault_id: rawBoost.vaultId,
      name: rawBoost.title,
      contract_address: normalizeAddressOrThrow(rawBoost.contractAddress),

      staked_token_address: normalizeAddressOrThrow(vault.contract_address),
      staked_token_decimals: vault.token_decimals,
      vault_want_address: normalizeAddressOrThrow(vault.want_address),
      vault_want_decimals: vault.want_decimals,

      reward_token_decimals: rewards[0].decimals,
      reward_token_symbol: rewards[0].symbol,
      reward_token_address: normalizeAddressOrThrow(rewards[0].address),
      reward_token_price_feed_key: rewards[0].oracleId,

      eol: rawBoost.status === "inactive",
      eol_date: eolDate,
    };
  } catch (error) {
    logger.error({ msg: "Could not map raw boost to expected format", data: { rawVault: rawBoost }, error });
    logger.debug(error);
    throw error;
  }
}

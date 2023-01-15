import { keyBy } from "lodash";
import * as path from "path";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { GITHUB_RO_AUTH_TOKEN, GIT_WORK_DIRECTORY } from "../../../utils/config";
import { normalizeAddress } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { GitFileVersion, gitStreamFileVersions } from "../../common/connector/git-file-history";
import { normalizeVaultId } from "../utils/normalize-vault-id";
import { BeefyVault } from "./vault-list";

const logger = rootLogger.child({ module: "beefy", component: "boost-list" });

export interface RawBeefyBoost {
  id: string;
  poolId: string;
  name: string;
  assets?: string[] | null;
  tokenAddress?: string | null;
  earnedToken: string;
  earnedTokenDecimals: number;
  earnedTokenAddress: string;
  earnContractAddress: string;
  earnedOracle: string;
  earnedOracleId: string;
  partnership: boolean;
  status: "active" | "closed";
}

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
      : "git@github.com:beefyfinance/beefy-v2.git",
    branch: "main",
    filePath: `src/config/boost/${chain}.json`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v2"),
    order: "old-to-recent",
    throwOnError: false,
    onePerMonth: false,
  });

  const v2$ = Rx.from(fileContentStreamV2).pipe(
    // parse the file content
    Rx.map((fileVersion) => {
      const boosts = JSON.parse(fileVersion.fileContent) as RawBeefyBoost[];
      return { fileVersion, boosts };
    }),
  );

  const vaultMap = keyBy(allChainVaults, (vault) => normalizeVaultId(vault.id));

  // process in chronological order
  return v2$.pipe(
    // process the vaults in chronolical order and mark the eol date if found
    Rx.reduce((acc, { fileVersion, boosts }) => {
      // reset the foundInCurrentBatch flag
      for (const boostId of Object.keys(acc)) {
        acc[boostId].foundInCurrentBatch = false;
      }

      // add vaults to the accumulator
      for (const boost of boosts) {
        const boostId = boost.id;
        if (!acc[boostId]) {
          const eolDate = boost.status === "closed" ? fileVersion.date : null;
          acc[boostId] = { fileVersion, eolDate, boost, foundInCurrentBatch: true };
        } else {
          if (acc[boostId].fileVersion.date > fileVersion.date) {
            logger.warn({
              msg: "Found a boost with a newer version in the past",
              data: {
                boostId,
                previousDate: acc[boostId].fileVersion.date,
                newDate: fileVersion.date,
                boost,
              },
            });
            continue;
          }

          const eolDate = boost.status === "closed" ? acc[boostId].eolDate || fileVersion.date : null;
          acc[boostId] = { boost, eolDate, foundInCurrentBatch: true, fileVersion };
        }
      }

      // mark all deleted vaults as eol if not already done
      for (const boostId of Object.keys(acc)) {
        if (!acc[boostId].foundInCurrentBatch) {
          acc[boostId].boost.status = "closed";
          acc[boostId].eolDate = acc[boostId].eolDate || fileVersion.date;
        }
      }

      return acc;
    }, {} as Record<string, { foundInCurrentBatch: boolean; fileVersion: GitFileVersion; eolDate: Date | null; boost: RawBeefyBoost }>),

    // flatten the accumulator
    Rx.map((acc) => Object.values(acc)),
    Rx.concatAll(),

    Rx.tap(({ fileVersion, boost, eolDate }) =>
      logger.trace({
        msg: "Boost from git history",
        data: { fileVersion: { ...fileVersion, fileContent: "<removed>" }, boost, isEol: boost.status === "closed", eolDate },
      }),
    ),

    Rx.tap(({ fileVersion, boost, eolDate }) => {
      if (boost.status === "closed" && !eolDate) {
        logger.error({
          msg: "product marked as eol but no eol date found",
          data: { fileVersion: { ...fileVersion, fileContent: "<removed>" }, boost, eolDate },
        });
      }
    }),

    // just emit the boost
    Rx.concatMap(({ boost, eolDate }) => {
      const vault = vaultMap[normalizeVaultId(boost.poolId)];
      if (!vault) {
        logger.error({ msg: "Could not find vault for boost", data: { boostId: boost.id, vaultId: normalizeVaultId(boost.poolId) } });
        return Rx.EMPTY;
      }
      return Rx.of(rawBoostToBeefyBoost(chain, boost, vault, eolDate));
    }),

    Rx.tap({
      complete: () => logger.debug({ msg: "Finished fetching boost list from beefy-v2 repo git history", data: { chain } }),
    }),
  );
}

function rawBoostToBeefyBoost(chain: Chain, rawBoost: RawBeefyBoost, vault: BeefyVault, eolDate: Date | null): BeefyBoost {
  try {
    return {
      id: rawBoost.id,
      chain,

      vault_id: rawBoost.poolId,
      name: rawBoost.name,
      contract_address: normalizeAddress(rawBoost.earnContractAddress),

      staked_token_address: normalizeAddress(vault.contract_address),
      staked_token_decimals: vault.token_decimals,
      vault_want_address: normalizeAddress(vault.want_address),
      vault_want_decimals: vault.want_decimals,

      reward_token_decimals: rawBoost.earnedTokenDecimals,
      reward_token_symbol: rawBoost.earnedToken,
      reward_token_address: normalizeAddress(rawBoost.earnedTokenAddress),
      reward_token_price_feed_key: rawBoost.earnedOracleId,

      eol: rawBoost.status === "closed",
      eol_date: eolDate,
    };
  } catch (error) {
    logger.error({ msg: "Could not map raw boost to expected format", data: { rawVault: rawBoost }, error });
    logger.debug(error);
    throw error;
  }
}

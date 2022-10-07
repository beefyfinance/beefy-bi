import { Chain } from "../../../types/chain";
import * as path from "path";
import { GITHUB_RO_AUTH_TOKEN, GIT_WORK_DIRECTORY } from "../../../utils/config";
import { normalizeAddress } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import * as Rx from "rxjs";
import { gitStreamFileVersions } from "../../common/connector/git-file-history";
import { BeefyVault } from "./vault-list";
import { normalizeVaultId } from "../utils/normalize-vault-id";

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

  staked_token_address: string;
  staked_token_decimals: number;
  vault_want_address: string;
  vault_want_decimals: number;

  reward_token_symbol: string;
  reward_token_decimals: number;
  reward_token_address: string;
  reward_token_price_feed_key: string;
}

export function beefyBoostsFromGitHistory$(chain: Chain, allChaiVaults: Record<string, BeefyVault>): Rx.Observable<BeefyBoost> {
  logger.debug({ msg: "Fetching boost list from beefy-v2 repo git history", data: { chain } });

  const fileContentStreamV2 = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-v2.git`
      : "git@github.com:beefyfinance/beefy-v2.git",
    branch: "main",
    filePath: `src/config/boost/${chain}.json`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v2"),
    order: "recent-to-old",
    throwOnError: false,
    onePerMonth: true,
  });

  const v2$ = Rx.from(fileContentStreamV2).pipe(
    // parse the file content
    Rx.concatMap((fileVersion) => {
      const boosts = JSON.parse(fileVersion.fileContent) as RawBeefyBoost[];
      const boostsAndVersion = boosts.map((boost) => ({ fileVersion, boost }));
      return Rx.from(boostsAndVersion);
    }),
  );

  return v2$.pipe(
    // only keep the latest version of each boost
    Rx.distinct(({ boost }) => boost.id), // remove duplicates

    // fix the status if we find a new boost not in the latest file version
    Rx.map(({ fileVersion, boost }) => {
      if (!fileVersion.latestVersion && boost.status !== "closed") {
        return { fileVersion, boost: { ...boost, status: "closed" } as typeof boost };
      }
      return { fileVersion, boost };
    }),

    // just emit the boost
    Rx.concatMap(({ boost }) => {
      const vault = allChaiVaults[normalizeVaultId(boost.poolId)];
      if (!vault) {
        logger.error({ msg: "Could not find vault for boost", data: { boostId: boost.id, vaultId: boost.poolId } });
        return Rx.EMPTY;
      }
      return Rx.of(rawBoostToBeefyBoost(chain, boost, vault));
    }),

    Rx.tap({
      complete: () => logger.debug({ msg: "Finished fetching boost list from beefy-v2 repo git history", data: { chain } }),
    }),
  );
}

function rawBoostToBeefyBoost(chain: Chain, rawBoost: RawBeefyBoost, vault: BeefyVault): BeefyBoost {
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
    };
  } catch (error) {
    logger.error({ msg: "Could not map raw boost to expected format", data: { rawVault: rawBoost }, error });
    logger.debug(error);
    throw error;
  }
}

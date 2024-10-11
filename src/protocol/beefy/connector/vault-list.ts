import { ethers } from "ethers";
import { cloneDeep } from "lodash";
import * as path from "path";
import prettier from "prettier";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { getChainWNativeTokenAddress } from "../../../utils/addressbook";
import { GITHUB_RO_AUTH_TOKEN, GIT_WORK_DIRECTORY } from "../../../utils/config";
import { normalizeAddressOrThrow } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { GitFileVersion, gitStreamFileVersions } from "../../common/connector/git-file-history";

const logger = rootLogger.child({ module: "beefy", component: "vault-list" });

interface RawBeefyVault {
  id: string;
  version?: number;
  token?: string;
  tokenAddress?: string;
  tokenDecimals: number;
  earnedTokenAddress: string;
  earnedTokenDecimals: number;
  earnContractAddress: string;
  earnedToken: string;
  isGovVault?: boolean;
  type?: "gov" | "standard" | "cowcentrated";
  oracleId: string;
  status?: string;
  assets?: string[];
  bridged?: Record<Chain, string>; // { "optimism": "0xc55E93C62874D8100dBd2DfE307EDc1036ad5434" },
  earningPoints?: boolean;
  platformId?: string;
}

interface BeefyBaseVaultConfig {
  id: string;
  chain: Chain;
  token_name: string;
  token_decimals: number;
  contract_address: string;
  want_address: string;
  want_decimals: number;
  eol: boolean;
  eol_date: Date | null;
  protocol: string;
  protocol_product: string;
  assets: string[];
  want_price_feed_key: string;
  platform_id?: string;
}

export interface BeefyGovVaultConfig extends BeefyBaseVaultConfig {
  is_gov_vault: true;
  bridged_version_of: null;
  gov_vault_reward_token_symbol: string;
  gov_vault_reward_token_address: string;
  gov_vault_reward_token_decimals: number;
}

export interface BeefyStdVaultConfig extends BeefyBaseVaultConfig {
  is_gov_vault: false;
  bridged_version_of: null;
  gov_vault_reward_token_symbol: null;
  gov_vault_reward_token_address: null;
  gov_vault_reward_token_decimals: null;
  earning_eigenlayer_points: boolean;
}

export interface BeefyBridgedVersionOfStdVault extends BeefyBaseVaultConfig {
  is_gov_vault: false;
  bridged_version_of: BeefyStdVaultConfig;
  gov_vault_reward_token_symbol: null;
  gov_vault_reward_token_address: null;
  gov_vault_reward_token_decimals: null;
}

export type BeefyVault = BeefyGovVaultConfig | BeefyStdVaultConfig | BeefyBridgedVersionOfStdVault;

function isBeefyStdVaultConfig(vault: BeefyVault): vault is BeefyStdVaultConfig {
  return !vault.is_gov_vault && vault.bridged_version_of === null;
}
export function isBeefyGovVaultConfig(vault: BeefyVault): vault is BeefyGovVaultConfig {
  return vault.is_gov_vault;
}
export function isBeefyBridgedVersionOfStdVaultConfig(vault: BeefyVault): vault is BeefyBridgedVersionOfStdVault {
  return !vault.is_gov_vault && vault.bridged_version_of !== null;
}

export function beefyVaultsFromGitHistory$(chain: Chain): Rx.Observable<BeefyVault> {
  logger.debug({ msg: "Fetching vault list from beefy-v2 repo git history", data: { chain } });

  const fileContentStreamV2 = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-v2.git`
      : "https://github.com/beefyfinance/beefy-v2.git",
    branch: "main",
    filePath: `src/config/vault/${chain}.json`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v2"),
    order: "old-to-recent",
    throwOnError: false,
    onePerMonth: false,
  });

  const v1Chain = chain === "avax" ? "avalanche" : chain;
  const fileContentStreamV1 = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-app.git`
      : "https://github.com/beefyfinance/beefy-app.git",
    branch: "prod",
    filePath: `src/features/configure/vault/${v1Chain}_pools.js`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v1"),
    order: "old-to-recent",
    throwOnError: false,
    onePerMonth: true,
  });

  const v2$ = Rx.from(fileContentStreamV2).pipe(
    // parse the file content
    Rx.map((fileVersion) => {
      const vaults = JSON.parse(fileVersion.fileContent) as RawBeefyVault[];
      return { fileVersion, vaults, error: false };
    }),
  );

  const v1$ = Rx.from(fileContentStreamV1).pipe(
    // parse the file content
    Rx.map((fileVersion) => {
      // using prettier to transform js objects into proper json was the easiest way for me
      try {
        // remove js code to make json5-like string
        const jsonCode = "[" + fileVersion.fileContent.trim().split("\n").slice(1, -1).join("\n") + "]";

        // format json with comments in a standard way to make comment removing easy
        const json5Content = prettier.format(jsonCode, {
          semi: false,
          parser: "json5",
          quoteProps: "consistent",
          singleQuote: false,
        });

        const jsonContent = prettier.format(
          json5Content
            .replace(/\/\*(\s|\S)*?\*\//gm, "") // remove multiline comments
            .replace(/\/\/( .*\n|\n)/gm, ""), // remove single line comments
          { parser: "json", semi: false },
        );

        const vaults: RawBeefyVault[] = JSON.parse(jsonContent);
        return { fileVersion, vaults, error: false };
      } catch (error) {
        logger.error({
          msg: "Could not parse vault list for v1 hash",
          data: { chain, commitHash: fileVersion.commitHash, date: fileVersion.date },
          error,
        });
        logger.debug(error);
        return { fileVersion, vaults: [], error: true };
      }
    }),
  );

  // process in chronological order
  return Rx.concat(v1$, v2$).pipe(
    Rx.filter(({ error }) => !error),

    Rx.pipe(
      // process the vaults in chronolical order and mark the eol date if found
      Rx.reduce((acc, { fileVersion, vaults }) => {
        // reset the foundInCurrentBatch flag
        for (const vaultAddress of Object.keys(acc)) {
          acc[vaultAddress].foundInCurrentBatch = false;
        }

        // add vaults to the accumulator
        for (const vault of vaults) {
          // ignore those without earned token address
          if (!vault.earnedTokenAddress) {
            const msg = { msg: "Could not find vault earned token address for vault", data: { vaultId: vault.id } };
            if (vault?.id?.endsWith("-rp") || vault?.id === "bifi-pool") {
              logger.debug(msg);
            } else {
              logger.error(msg);
            }
            logger.trace(vault);
            continue;
          }

          // index by contract address since beefy's team changes ids when the vault is eol
          if (!ethers.utils.isAddress(vault.earnContractAddress)) {
            logger.error({
              msg: "Vault earnContractAddress is invalid, ignoring",
              data: { vaultId: vault.id, earnContractAddress: vault.earnContractAddress },
            });
            logger.trace(vault);
            continue;
          }
          const vaultAddress = normalizeAddressOrThrow(vault.earnContractAddress);
          if (!acc[vaultAddress]) {
            const eolDate = vault.status === "eol" ? fileVersion.date : null;
            acc[vaultAddress] = { fileVersion, eolDate, vault, foundInCurrentBatch: true };
          } else {
            acc[vaultAddress].foundInCurrentBatch = true;

            const eolDate = vault.status === "eol" ? acc[vaultAddress].eolDate || fileVersion.date : null;
            acc[vaultAddress] = { vault, eolDate, foundInCurrentBatch: true, fileVersion };
          }
        }

        // mark all deleted vaults as eol if not already done
        for (const vaultAddress of Object.keys(acc)) {
          if (!acc[vaultAddress].foundInCurrentBatch) {
            acc[vaultAddress].vault.status = "eol";
            acc[vaultAddress].eolDate = acc[vaultAddress].eolDate || fileVersion.date;
          }
        }

        return acc;
      }, {} as Record<string, { foundInCurrentBatch: boolean; fileVersion: GitFileVersion; eolDate: Date | null; vault: RawBeefyVault }>),
      // flatten the accumulator
      Rx.map((acc) => Object.values(acc)),
      Rx.concatAll(),

      Rx.tap(({ fileVersion, vault, eolDate }) =>
        logger.trace({
          msg: "Vault from git history",
          data: { fileVersion: { ...fileVersion, fileContent: "<removed>" }, vault, isEol: vault.status === "eol", eolDate },
        }),
      ),

      Rx.tap(({ fileVersion, vault, eolDate }) => {
        if (vault.status === "eol" && !eolDate) {
          logger.error({
            msg: "product marked as eol but no eol date found",
            data: { fileVersion: { ...fileVersion, fileContent: "<removed>" }, vault, eolDate },
          });
        }
      }),

      // exclude CLM related products
      Rx.filter(({ vault }) => {
        const isClm = vault.type === "cowcentrated";
        const isRewardPool =
          vault.type === "gov" &&
          vault.version &&
          vault.version >= 2 &&
          vault.token &&
          vault.token.startsWith("cow") &&
          vault.earnedToken.startsWith("rCow");
        return !isRewardPool && !isClm;
      }),

      // just emit the vault
      Rx.map(({ vault, eolDate }) => ({ vault: rawVaultToBeefyVault(chain, vault, eolDate), rawVault: vault })),
    ),

    // ignore cowcentrated vaults, those are covered by thegraph
    Rx.filter(({ rawVault }) => rawVault.type !== "cowcentrated"),

    // add a virtual product of the bridged version of the vault
    Rx.map(({ vault, rawVault }) => {
      if (!rawVault.bridged) {
        return [vault];
      }

      if (!isBeefyStdVaultConfig(vault)) {
        logger.error({ msg: "Expected vault to be a standard vault", data: { vault } });
        return [vault];
      }

      type K = keyof Required<RawBeefyVault>["bridged"];
      type V = Required<RawBeefyVault>["bridged"][K];
      const bridgedVaults = (Object.entries(rawVault.bridged) as [K, V][])
        .map(([chain, address]) => ({ chain, address }))
        .filter((p): p is { chain: Chain; address: string } => p.address !== null)
        .map(({ chain, address }): BeefyBridgedVersionOfStdVault => {
          const bridgedMooBifiVault: BeefyBridgedVersionOfStdVault = {
            ...cloneDeep(vault),
            id: `${chain}-bridged-${vault.id}`,
            is_gov_vault: false,
            bridged_version_of: vault,
            contract_address: address,
            chain,
            token_name: `${chain} ${vault.token_name}`,
            token_decimals: 18,
          };
          return bridgedMooBifiVault;
        });
      return [vault, ...bridgedVaults];
    }),

    Rx.concatAll(),

    Rx.tap({
      complete: () => logger.debug({ msg: "Finished fetching vault list from beefy-v2 repo git history", data: { chain } }),
    }),
  );
}

function rawVaultToBeefyVault(chain: Chain, rawVault: RawBeefyVault, eolDate: Date | null): BeefyVault {
  try {
    const wnative = getChainWNativeTokenAddress(chain);

    // try to extract protocol
    let protocol = rawVault.oracleId;
    let protocol_product = rawVault.oracleId;
    if (rawVault.oracleId.includes("-")) {
      const parts = rawVault.oracleId.split("-");
      protocol = parts[0];
      protocol_product = parts.slice(1).join("-");
    } else if (rawVault.oracleId.startsWith("be")) {
      protocol = "beefy";
      protocol_product = rawVault.oracleId;
    } else if (rawVault.oracleId === "BIFI" || rawVault.oracleId === "oldBIFI") {
      protocol = "beefy";
      protocol_product = rawVault.oracleId;
    } else {
      protocol = "unknown";
      protocol_product = rawVault.oracleId;
    }

    return {
      id: rawVault.id,
      chain,
      token_name: rawVault.earnedToken,
      token_decimals: 18,
      contract_address: normalizeAddressOrThrow(rawVault.earnContractAddress),
      want_address: normalizeAddressOrThrow(rawVault.tokenAddress || wnative),
      want_decimals: rawVault.tokenDecimals,
      eol: rawVault.status === "eol",
      eol_date: eolDate,
      assets: rawVault.assets || [],
      protocol,
      protocol_product,
      want_price_feed_key: rawVault.oracleId,
      bridged_version_of: null,
      platform_id: rawVault.platformId,
      ...(rawVault.isGovVault || rawVault.type === "gov"
        ? {
            is_gov_vault: true,
            gov_vault_reward_token_symbol: rawVault.earnedToken,
            gov_vault_reward_token_address: rawVault.earnedTokenAddress,
            gov_vault_reward_token_decimals: rawVault.earnedTokenDecimals,
          }
        : {
            is_gov_vault: false,
            gov_vault_reward_token_symbol: null,
            gov_vault_reward_token_address: null,
            gov_vault_reward_token_decimals: null,
            earning_eigenlayer_points: rawVault?.earningPoints === true,
          }),
    };
  } catch (error) {
    logger.error({ msg: "Could not map raw vault to expected format", data: { rawVault }, error });
    logger.debug(error);
    throw error;
  }
}

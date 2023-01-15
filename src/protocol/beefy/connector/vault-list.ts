import * as path from "path";
import prettier from "prettier";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { getChainWNativeTokenAddress } from "../../../utils/addressbook";
import { GITHUB_RO_AUTH_TOKEN, GIT_WORK_DIRECTORY } from "../../../utils/config";
import { normalizeAddress } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { GitFileVersion, gitStreamFileVersions } from "../../common/connector/git-file-history";
import { normalizeVaultId } from "../utils/normalize-vault-id";

const logger = rootLogger.child({ module: "beefy", component: "vault-list" });

interface RawBeefyVault {
  id: string;
  tokenAddress?: string;
  tokenDecimals: number;
  earnedTokenAddress: string;
  earnedTokenDecimals: number;
  earnContractAddress: string;
  earnedToken: string;
  isGovVault?: boolean;
  oracleId: string;
  status?: string;
  assets?: string[];
}

export interface BeefyVault {
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
  is_gov_vault: boolean;
  gov_vault_reward_token_symbol: string | null;
  gov_vault_reward_token_address: string | null;
  gov_vault_reward_token_decimals: number | null;
}

export function beefyVaultsFromGitHistory$(chain: Chain): Rx.Observable<BeefyVault> {
  logger.debug({ msg: "Fetching vault list from beefy-v2 repo git history", data: { chain } });

  const fileContentStreamV2 = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-v2.git`
      : "git@github.com:beefyfinance/beefy-v2.git",
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
      : "git@github.com:beefyfinance/beefy-app.git",
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

    // process the vaults in chronolical order and mark the eol date if found
    Rx.reduce((acc, { fileVersion, vaults }) => {
      // reset the foundInCurrentBatch flag
      for (const vaultId of Object.keys(acc)) {
        acc[vaultId].foundInCurrentBatch = false;
      }

      // add vaults to the accumulator
      for (const vault of vaults) {
        // ignore those without earned token address
        if (!vault.earnedTokenAddress) {
          logger.error({ msg: "Could not find vault earned token address for vault", data: { vaultId: vault.id } });
          logger.trace(vault);
          continue;
        }

        const vaultId = normalizeVaultId(vault.id);
        if (!acc[vaultId]) {
          const eolDate = vault.status === "eol" ? fileVersion.date : null;
          acc[vaultId] = { fileVersion, eolDate, vault, foundInCurrentBatch: true };
        } else {
          if (acc[vaultId].fileVersion.date > fileVersion.date) {
            logger.error({
              msg: "Found a vault with a newer version in the past",
              data: { vaultId, vault, fileVersion, previousFileVersion: acc[vaultId].fileVersion },
            });
          }

          const eolDate = acc[vaultId].eolDate || (vault.status === "eol" ? fileVersion.date : null);
          acc[vaultId] = { vault, eolDate, foundInCurrentBatch: true, fileVersion };
        }
      }

      // mark all deleted vaults as eol if not already done
      for (const vaultId of Object.keys(acc)) {
        if (!acc[vaultId].foundInCurrentBatch && !acc[vaultId].eolDate) {
          acc[vaultId].eolDate = fileVersion.date;
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

    // just emit the vault
    Rx.map(({ vault, eolDate }) => rawVaultToBeefyVault(chain, vault, eolDate)),

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
    } else if (rawVault.oracleId === "BIFI") {
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
      contract_address: normalizeAddress(rawVault.earnContractAddress),
      want_address: normalizeAddress(rawVault.tokenAddress || wnative),
      want_decimals: rawVault.tokenDecimals,
      eol: rawVault.status === "eol",
      eol_date: eolDate,
      assets: rawVault.assets || [],
      protocol,
      protocol_product,
      want_price_feed_key: rawVault.oracleId,
      is_gov_vault: rawVault.isGovVault || false,
      gov_vault_reward_token_symbol: rawVault.isGovVault ? rawVault.earnedToken : null,
      gov_vault_reward_token_address: rawVault.isGovVault ? rawVault.earnedTokenAddress : null,
      gov_vault_reward_token_decimals: rawVault.isGovVault ? rawVault.earnedTokenDecimals : null,
    };
  } catch (error) {
    logger.error({ msg: "Could not map raw vault to expected format", data: { rawVault }, error });
    logger.debug(error);
    throw error;
  }
}

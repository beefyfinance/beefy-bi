import * as path from "path";
import prettier from "prettier";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { getChainWNativeTokenAddress } from "../../../utils/addressbook";
import { GITHUB_RO_AUTH_TOKEN, GIT_WORK_DIRECTORY } from "../../../utils/config";
import { normalizeAddress } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { gitStreamFileVersions } from "../../common/connector/git-file-history";
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
    order: "recent-to-old",
    throwOnError: false,
    onePerMonth: true,
  });

  const v1Chain = chain === "avax" ? "avalanche" : chain;
  const fileContentStreamV1 = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-app.git`
      : "git@github.com:beefyfinance/beefy-app.git",
    branch: "prod",
    filePath: `src/features/configure/vault/${v1Chain}_pools.js`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v1"),
    order: "recent-to-old",
    throwOnError: false,
    onePerMonth: true,
  });

  const v2$ = Rx.from(fileContentStreamV2).pipe(
    // parse the file content
    Rx.concatMap((fileVersion) => {
      const vaults = JSON.parse(fileVersion.fileContent) as RawBeefyVault[];
      const vaultsAndVersion = vaults.map((vault) => ({ fileVersion, vault }));
      return Rx.from(vaultsAndVersion);
    }),
  );

  const v1$ = Rx.from(fileContentStreamV1).pipe(
    // parse the file content
    Rx.concatMap((fileVersion) => {
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
        const vaultsAndVersion = vaults.map((vault) => ({ fileVersion, vault }));
        return Rx.from(vaultsAndVersion);
      } catch (error) {
        logger.error({
          msg: "Could not parse vault list for v1 hash",
          data: { chain, commitHash: fileVersion.commitHash, date: fileVersion.date },
          error,
        });
        logger.debug(error);
        return Rx.EMPTY;
      }
    }),
  );

  return Rx.concat(v2$, v1$).pipe(
    // remove those without earned token address
    Rx.filter(({ vault }) => {
      if (!vault.earnedTokenAddress) {
        logger.error({ msg: "Could not find vault earned token address for vault", data: { vaultId: vault.id } });
        logger.trace(vault);
        return false;
      }
      return true;
    }),

    // only keep the latest version of each vault
    Rx.distinct(({ vault }) => normalizeVaultId(vault.id)), // remove duplicates

    // fix the status if we find a new vault not in the latest file version
    Rx.map(({ fileVersion, vault }) => {
      if (!fileVersion.latestVersion && vault.status !== "eol") {
        return { fileVersion, vault: { ...vault, status: "eol" } };
      }
      return { fileVersion, vault };
    }),

    Rx.tap(({ fileVersion, vault }) =>
      logger.trace({
        msg: "Vault from git history",
        data: { fileVersion: { ...fileVersion, fileContent: "<removed>" }, vault },
      }),
    ),

    // just emit the vault
    Rx.map(({ vault }) => rawVaultToBeefyVault(chain, vault)),

    Rx.tap({
      complete: () => logger.debug({ msg: "Finished fetching vault list from beefy-v2 repo git history", data: { chain } }),
    }),
  );
}

function rawVaultToBeefyVault(chain: Chain, rawVault: RawBeefyVault): BeefyVault {
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

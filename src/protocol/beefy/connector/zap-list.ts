import * as path from "path";
import prettier from "prettier";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { GITHUB_RO_AUTH_TOKEN, GIT_WORK_DIRECTORY } from "../../../utils/config";
import { normalizeAddress } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { gitStreamFileVersions } from "../../common/connector/git-file-history";
import { isNotNull } from "../../common/utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "zap-list" });

interface RawBeefyZap {
  chainId: Chain;
  ammId: string;
  zapAddress: string;
}

export interface BeefyZap {
  chain: Chain;
  address: string;
}

export function beefyZapsFromGit$(): Rx.Observable<BeefyZap> {
  logger.debug({ msg: "Fetching zap list from beefy-v2 repo git history" });

  const beefyZapStream = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-v2.git`
      : "https://github.com/beefyfinance/beefy-v2.git",
    branch: "main",
    filePath: `src/config/zap/beefy.ts`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v2"),
    order: "recent-to-old",
    throwOnError: false,
    onePerMonth: false,
  });

  const oneInchZapStream = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-v2.git`
      : "https://github.com/beefyfinance/beefy-v2.git",
    branch: "main",
    filePath: `src/config/zap/one-inch.ts`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v2"),
    order: "recent-to-old",
    throwOnError: false,
    onePerMonth: false,
  });

  // only get the latest version of each file
  const zapFiles$ = Rx.from([beefyZapStream.next(), oneInchZapStream.next()]).pipe(
    Rx.mergeMap((p) => p), // await
    Rx.map((p) => (p.done ? null : p.value)),
    Rx.filter(isNotNull),
  );

  return zapFiles$.pipe(
    // parse the file content
    Rx.mergeMap((fileVersion) => {
      let content = fileVersion.fileContent;

      // move to the position of the first array element, skipping the import and type definitions
      const lines = content.split("\n");
      const firstArrayElementIndex = lines.findIndex((line) => line.includes("["));

      // remove all other lines, necessary because there is computation in the file that we don't want to deal with
      content = lines
        .slice(firstArrayElementIndex)
        .filter((line) => !line.startsWith("import"))
        .filter((line) => line.includes("zapAddress") || line.includes("chainId") || line.includes("{") || line.includes("}"))
        .join("\n");

      content = `[${content}]`;

      // format the thing as json, since we come from a ts file
      content = prettier.format(content, {
        semi: false,
        parser: "json5",
        quoteProps: "consistent",
        singleQuote: false,
      });

      content = prettier.format(content, { parser: "json", semi: false });

      const zaps = JSON.parse(content) as RawBeefyZap[];
      return zaps;
    }),

    Rx.map((zap) => ({
      chain: zap.chainId,
      address: normalizeAddress(zap.zapAddress),
    })),

    Rx.tap({
      complete: () => logger.debug({ msg: "Finished fetching zap list from beefy-v2 repo git history" }),
    }),
  );
}

import { cloneDeep } from "lodash";
import * as Rx from "rxjs";
import yargs from "yargs";
import { getVaultFallbackCreationInfos, VAULT_CREATED_AT_BUFFER_SECONDS } from "../protocol/beefy/connector/vault-created-at";
import { fetchContractCreationInfos$ } from "../protocol/common/connector/contract-creation";
import { createBatchStreamConfig, defaultImportBehaviour, ImportBehaviour, ImportCtx } from "../protocol/common/types/import-context";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { allChainIds, Chain } from "../types/chain";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

// Compares the beefy api createdAt fallback with the explorer creation block for some vaults.
// Usage:
//   node -r ts-node/register -r dotenv/config ./src/script/test-vault-fallback-creation.ts -c base -a 0xabc... 0xdef...
// Options:
//   --bufferDays   how many days before createdAt to start (default 30)
//   --explorer     also query the explorer to compare (default true)
async function main() {
  const argv = await yargs.usage("$0 [args]").options({
    chain: {
      type: "string",
      choices: allChainIds,
      alias: "c",
      demand: true,
      describe: "chain of the vaults",
    },
    contractAddress: {
      type: "string",
      array: true,
      demand: true,
      alias: "a",
      describe: "vault contract addresses (earnContractAddress in the beefy api)",
    },
    bufferDays: {
      type: "number",
      default: VAULT_CREATED_AT_BUFFER_SECONDS / (24 * 60 * 60),
      describe: "how many days before createdAt to start",
    },
    explorer: {
      type: "boolean",
      default: true,
      describe: "also query the explorer to compare the creation blocks",
    },
  }).argv;

  const chain = argv.chain as Chain;
  const addresses = argv.contractAddress;
  const bufferSeconds = argv.bufferDays * 24 * 60 * 60;

  const behaviour: ImportBehaviour = { ...cloneDeep(defaultImportBehaviour), mode: "historical" };
  const rpcConfig = getMultipleRpcConfigsForChain({ chain, behaviour })[0];
  const ctx: ImportCtx = {
    chain,
    client: {} as any, // unused
    behaviour,
    rpcConfig,
    streamConfig: createBatchStreamConfig(chain, behaviour),
  };

  // explorer results, keyed by address
  const explorerByAddress = new Map<string, { blockNumber: number; datetime: Date } | null>();
  if (argv.explorer) {
    const pipeline$ = Rx.from(addresses).pipe(
      fetchContractCreationInfos$({
        ctx,
        getCallParams: (contractAddress) => ({ chain, contractAddress }),
        formatOutput: (contractAddress, infos) => ({ contractAddress, infos }),
      }),
      Rx.tap(({ contractAddress, infos }) => explorerByAddress.set(contractAddress, infos)),
    );
    await consumeObservable(pipeline$);
  }

  const rows: Record<string, unknown>[] = [];
  for (const contractAddress of addresses) {
    const row: Record<string, unknown> = { contract: contractAddress };
    try {
      const fallback = await getVaultFallbackCreationInfos(ctx, contractAddress, bufferSeconds);
      if (!fallback) {
        row.result = "no fallback (vault not in beefy api or no block found)";
      } else {
        row.createdAt = new Date(fallback.createdAt * 1000).toISOString();
        row.startFrom = fallback.datetime.toISOString();
        row.fallbackBlock = fallback.blockNumber;
      }
    } catch (error) {
      row.result = `error: ${(error as Error).message}`;
    }

    if (argv.explorer) {
      const explorer = explorerByAddress.get(contractAddress);
      if (!explorer) {
        row.explorerBlock = "none";
      } else {
        row.explorerBlock = explorer.blockNumber;
        row.explorerDate = explorer.datetime.toISOString();
        if (typeof row.fallbackBlock === "number") {
          // positive = fallback starts this many blocks before the explorer creation block
          const blocksBefore = explorer.blockNumber - row.fallbackBlock;
          row.blocksBefore = blocksBefore;
          row.check = blocksBefore >= 0 ? "ok" : "FALLBACK AFTER EXPLORER (would miss deposits)";
        }
      }
    }
    rows.push(row);
  }

  console.table(rows);
}

runMain(main);

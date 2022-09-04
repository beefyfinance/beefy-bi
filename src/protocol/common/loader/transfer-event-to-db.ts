import { ethers } from "ethers";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { db_query, mapAddressToEvmAddressId, mapTransactionToEvmTransactionId } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger2";
import { TokenizedVaultUserTransfer } from "../../types/connector";
import { mapBlockDatetime } from "../connector/block-datetime";
import { mapERC20TokenBalance } from "../connector/owner-balance";

const logger = rootLogger.child({ module: "import-script", component: "transferEventToDb" });

export const transferEventToDb: (
  client: PoolClient,
  chain: Chain,
  provider: ethers.providers.JsonRpcProvider,
) => Rx.OperatorFunction<TokenizedVaultUserTransfer, TokenizedVaultUserTransfer> =
  (client, chain, provider) => (transfers$) => {
    return (
      transfers$
        .pipe(
          Rx.tap((userAction) => logger.debug({ msg: "processing user action", data: { chain, userAction } })),

          // remove mint burn events
          Rx.filter((transfer) => transfer.ownerAddress !== "0x0000000000000000000000000000000000000000"),

          // batch transfer events before fetching additional infos
          Rx.bufferCount(200),

          // we need the balance of each owner
          mapERC20TokenBalance(
            chain,
            provider,
            (t) => ({
              blockNumber: t.blockNumber,
              decimals: t.sharesDecimals,
              contractAddress: t.vaultAddress,
              ownerAddress: t.ownerAddress,
            }),
            "ownerBalance",
          ),

          // we also need the date of each block
          mapBlockDatetime(provider, (t) => t.blockNumber, "blockDatetime"),

          // we want to catch any errors from the RPC
          Rx.catchError((error) => {
            logger.error({ msg: "error importing latest chain data", data: { chain, error } });
            return Rx.EMPTY;
          }),

          // flatten the resulting array
          Rx.mergeMap((transfers) => Rx.from(transfers)),
        )
        // insert relational data pipe
        .pipe(
          // batch by an amount more suitable for batch inserts
          Rx.bufferCount(2000),

          Rx.tap((transfers) =>
            logger.info({
              msg: "inserting transfer batch",
              data: { chain, count: transfers.length },
            }),
          ),

          // insert the owner addresses
          mapAddressToEvmAddressId(
            client,
            (transfer) => ({ chain, address: transfer.ownerAddress, metadata: {} }),
            "owner_evm_address_id",
          ),

          // fetch the vault addresses
          mapAddressToEvmAddressId(
            client,
            (transfer) => ({ chain, address: transfer.vaultAddress, metadata: {} }),
            "vault_evm_address_id",
          ),

          // insert the transactions if needed
          mapTransactionToEvmTransactionId(
            client,
            (transfer) => ({
              chain,
              hash: transfer.transactionHash,
              block_number: transfer.blockNumber,
              block_datetime: transfer.blockDatetime,
            }),
            "evm_transaction_id",
          ),
        )
        // insert the actual shares updates data
        .pipe(
          // insert to the vault table
          Rx.mergeMap(async (transfers) => {
            // short circuit if there's nothing to do
            if (transfers.length === 0) {
              return [];
            }

            await db_query<{ beefy_vault_id: number }>(
              `INSERT INTO vault_shares_transfer_ts (
                datetime,
                chain,
                block_number,
                evm_transaction_id,
                owner_evm_address_id,
                vault_evm_address_id,
                shares_balance_diff,
                shares_balance_after
                ) VALUES %L
                ON CONFLICT (owner_evm_address_id, vault_evm_address_id, evm_transaction_id, datetime) 
                DO NOTHING`,
              [
                transfers.map((transfer) => [
                  transfer.blockDatetime.toISOString(),
                  transfer.chain,
                  transfer.blockNumber,
                  transfer.evm_transaction_id,
                  transfer.owner_evm_address_id,
                  transfer.vault_evm_address_id,
                  transfer.sharesBalanceDiff.toString(),
                  transfer.ownerBalance.toString(),
                ]),
              ],
              client,
            );
            return transfers;
          }),

          Rx.tap((transfers) =>
            logger.debug({ msg: "done processing chain batch", data: { chain, count: transfers.length } }),
          ),

          // re-flatten the output if anyone is interested in processing further
          Rx.mergeMap((transfers) => Rx.from(transfers)),
        )
    );
  };

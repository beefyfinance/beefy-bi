import { logger } from "../utils/logger";
import {
  db_query,
  insertErc20TransferBatch,
  prepareInsertErc20TransferBatch,
} from "../utils/db";
import { batchAsyncStream } from "../utils/batch";
import { streamERC20TransferEvents } from "../lib/streamContractEvents";
import {
  getFirstTransactionInfos,
  getLastTransactionInfos,
} from "../lib/contract-transaction-infos";
import { getBlockDate } from "../utils/ethers";

async function main() {
  const chain = "fantom";
  const wnativeContractAddress = "0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83";
  //const contractAddress = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
  const contractAddress = "0x41D44B276904561Ac51855159516FD4cB2c90968";

  // get all strategies
  const strategyAddressesRows = await db_query<{ strategy_address: string }>(
    `
    select strategy_address 
    from vault_strategy
    where chain = %L and contract_address = %L
  `,
    [chain, contractAddress]
  );

  // prepare insert batch
  for (const strategyAddressRow of strategyAddressesRows) {
    await prepareInsertErc20TransferBatch(
      chain,
      strategyAddressRow.strategy_address
    );
    const { blockNumber: startBlock } = await getFirstTransactionInfos(
      chain,
      strategyAddressRow.strategy_address
    );
    const { blockNumber: endBlock } = await getLastTransactionInfos(
      chain,
      strategyAddressRow.strategy_address
    );

    // get wnative transfers from this address
    const stream = streamERC20TransferEvents(chain, wnativeContractAddress, {
      from: strategyAddressRow.strategy_address,
      startBlock,
      endBlock,
      //blockBatchSize: 100, // too many logs to process on wnative token, so reduce the block range
      timeOrder: "reverse", // we are more interested in the recent history
    });

    for await (const eventBatch of batchAsyncStream(stream, 100)) {
      const events = await Promise.all(
        eventBatch.map(async (event) => ({
          block_number: event.blockNumber,
          chain: chain,
          contract_address: contractAddress,
          from_address: event.data.from,
          to_address: event.data.to,
          time: (
            await getBlockDate(chain, event.blockNumber)
          ).datetime.toISOString(),
          value: event.data.value.toString(),
        }))
      );
      await insertErc20TransferBatch(events);
    }
  }
}

main()
  .then(() => {
    logger.info("Done");
    process.exit(0);
  })
  .catch((e) => {
    console.log(e);
    logger.error(e);
    process.exit(1);
  });

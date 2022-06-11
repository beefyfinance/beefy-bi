import { logger } from "../utils/logger";
import {
  insertErc20TransferBatch,
  prepareInsertErc20TransferBatch,
} from "../utils/db";
import { batchAsyncStream } from "../utils/batch";
import { streamERC20TransferEvents } from "../lib/streamContractEvents";
import { getBlockDate } from "../utils/ethers";

async function main() {
  const chain = "fantom";
  const contractAddress = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
  //const contractAddress = "0x41D44B276904561Ac51855159516FD4cB2c90968";

  await prepareInsertErc20TransferBatch(chain, contractAddress);

  const stream = streamERC20TransferEvents(chain, contractAddress);

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

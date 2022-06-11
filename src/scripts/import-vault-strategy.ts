import { logger } from "../utils/logger";
import { insertVaulStrategyBatch } from "../utils/db";
import { batchAsyncStream } from "../utils/batch";
import { streamBifiVaultUpgradeStratEvents } from "../lib/streamContractEvents";
import { getBlockDate } from "../utils/ethers";

async function main() {
  const chain = "fantom";
  //const contractAddress = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
  const contractAddress = "0x41D44B276904561Ac51855159516FD4cB2c90968";

  const stream = streamBifiVaultUpgradeStratEvents(chain, contractAddress);

  for await (const eventBatch of batchAsyncStream(stream, 100)) {
    const events = await Promise.all(
      eventBatch.map(async (event) => ({
        block_number: event.blockNumber,
        time: (
          await getBlockDate(chain, event.blockNumber)
        ).datetime.toISOString(),
        chain: chain,
        contract_address: contractAddress,
        strategy_address: event.data.implementation,
      }))
    );
    await insertVaulStrategyBatch(events);
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

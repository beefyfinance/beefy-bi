import { logger } from "../utils/logger";
import {
  insertErc20TransferBatch,
  prepareInsertErc20TransferBatch,
} from "../utils/db";
import { batchAsyncStream } from "../utils/batch";
import { streamBifiVaultUpgradeStratEvents } from "../lib/streamContractEvents";
import { ethers } from "ethers";

async function main() {
  const chain = "fantom";
  const contractAddress = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
  //const contractAddress = "0x41D44B276904561Ac51855159516FD4cB2c90968";
  console.log(
    ethers.utils.getAddress("0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83"),
    "0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83" ===
      ethers.utils.getAddress("0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83")
  );
  const stream = streamBifiVaultUpgradeStratEvents(chain, contractAddress);

  for await (const event of stream) {
    console.log(event);
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

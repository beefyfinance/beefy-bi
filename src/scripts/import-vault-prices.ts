import { logger } from "../utils/logger";
import { batchAsyncStream } from "../utils/batch";
import { streamBeefyVaultPrices } from "../lib/streamBeefyVaultPrices";
import { insertTokenPriceBatch } from "../utils/db";

async function main() {
  const chain = "fantom";
  //const contractAddress = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
  //const priceOracleId = "tomb-usdc-fusdt";

  const contractAddress = "0x41D44B276904561Ac51855159516FD4cB2c90968";
  const priceOracleId = "boo-ftm-usdc";

  const stream = streamBeefyVaultPrices(chain, priceOracleId);

  for await (const priceBatch of batchAsyncStream(stream, 100)) {
    await insertTokenPriceBatch(
      priceBatch.map((price) => ({
        chain: chain,
        contract_address: contractAddress,
        price: price.value,
        time: price.datetime.toISOString(),
      }))
    );
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

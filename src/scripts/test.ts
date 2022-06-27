import { logger } from "../utils/logger";
import _ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";

async function main() {
  const chain = "bsc";
  //const contractAddress = "0x07fFC2258c99e6667235fEAa90De35A0a50CFBFd";
  const contractAddress = "0x7828ff4aba7aab932d8407c78324b069d24284c9";

  const abi = ["function strategy() view external returns (address)"];
  const iface = new ethers.utils.Interface(abi);
  const data = iface.encodeFunctionData("strategy");
  console.log(data);
  console.log(ethers.utils.hexValue("0x01234"));
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

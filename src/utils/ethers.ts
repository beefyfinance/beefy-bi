import { RPC_URLS } from "./config";
import { Chain } from "../types/chain";
import * as ethers from "ethers";

function getProvider(chain: Chain) {
  return new ethers.providers.JsonRpcProvider(RPC_URLS[chain]);
}

export function getContract(
  chain: Chain,
  abi: ethers.ContractInterface,
  address: string
) {
  return new ethers.Contract(address, abi, getProvider(chain));
}

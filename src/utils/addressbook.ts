import { Chain } from "../types/chain";
import { normalizeAddress } from "./ethers";
import * as addressbook from "blockchain-addressbook";

export function getChainWNativeTokenAddress(chain: Chain): string {
  const addrBookChain =
    chain === "harmony" ? "one" : chain === "syscoin" ? "sys" : chain;
  return normalizeAddress(
    addressbook.addressBook[addrBookChain].tokens.WNATIVE.address
  );
}

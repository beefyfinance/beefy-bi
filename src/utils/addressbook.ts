import * as addressbook from "blockchain-addressbook";
import { Chain } from "../types/chain";
import { normalizeAddressOrThrow } from "./ethers";

function getAddressBookTokensConfig(chain: Chain) {
  if (chain === "base") {
    throw new Error("base chain is not in the addressbook yet");
  }
  const addrBookChain = chain === "harmony" ? "one" : chain;
  return addressbook.addressBook[addrBookChain].tokens;
}

export function getChainNetworkId(chain: Chain): number {
  if (chain === "base") {
    return 8453;
  }
  const addrBookChain = chain === "harmony" ? "one" : chain;
  return addressbook.ChainId[addrBookChain];
}

export function getChainWNativeTokenAddress(chain: Chain): string {
  if (chain === "base") {
    return "0x4200000000000000000000000000000000000006";
  }
  const tokens = getAddressBookTokensConfig(chain);
  return normalizeAddressOrThrow(tokens.WNATIVE.address);
}

export function getChainWNativeTokenSymbol(chain: Chain): string {
  if (chain === "base") {
    return "WETH";
  }
  const tokens = getAddressBookTokensConfig(chain);
  return tokens.WNATIVE.symbol;
}

import * as addressbook from "blockchain-addressbook";
import { Chain } from "../types/chain";
import { normalizeAddressOrThrow } from "./ethers";

export function getBridgedMooBifiTokenAddress(chain: Chain): string|null {
  // @todo: pull from the addressbook when live
  if (chain === "arbitrum") {
    return "0x508c6cF93e7D6793d7dB8b8B01ac6752A4275d75"
  } else if (chain === "optimism") {
    return "0x665E21ce21B1e7c7401647c1fb740981b270b71d"
  } else {
    return null;
  }
}

function getAddressBookTokensConfig(chain: Chain) {
  const addrBookChain = chain === "harmony" ? "one" : chain;
  return addressbook.addressBook[addrBookChain].tokens;
}

export function getChainNetworkId(chain: Chain): number {
  const addrBookChain = chain === "harmony" ? "one" : chain;
  return addressbook.ChainId[addrBookChain];
}

export function getChainWNativeTokenAddress(chain: Chain): string {
  const tokens = getAddressBookTokensConfig(chain);
  return normalizeAddressOrThrow(tokens.WNATIVE.address);
}

export function getChainWNativeTokenSymbol(chain: Chain): string {
  const tokens = getAddressBookTokensConfig(chain);
  return tokens.WNATIVE.symbol;
}

import * as addressbook from "blockchain-addressbook";
import { Chain } from "../types/chain";
import { normalizeAddressOrThrow } from "./ethers";

export function getBridgedMooBifiTokenAddress(chain: Chain): string | null {
  // @todo: pull from the addressbook when live
  if (chain === "optimism") {
    return "0xc55E93C62874D8100dBd2DfE307EDc1036ad5434";
  } else {
    return null;
  }
}
export function getBridgedVaultTargetChains(): Chain[] {
  return ["optimism"];
}

export function getBridgedVaultOriginChains(): Chain[] {
  return ["ethereum"];
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

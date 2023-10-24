import * as addressbook from "blockchain-addressbook";
import { Chain } from "../types/chain";
import { normalizeAddressOrThrow } from "./ethers";

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

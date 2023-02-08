import * as addressbook from "blockchain-addressbook";
import { Chain } from "../types/chain";
import { normalizeAddressOrThrow } from "./ethers";

function getAddressBookTokensConfig(chain: Chain) {
  const addrBookChain = chain === "harmony" ? "one" : chain === "syscoin" ? "sys" : chain;
  return addressbook.addressBook[addrBookChain].tokens;
}

export function getChainNetworkId(chain: Chain): number {
  const addrBookChain = chain === "harmony" ? "one" : chain === "syscoin" ? "sys" : chain;
  return addressbook.ChainId[addrBookChain];
}

export function getChainWNativeTokenAddress(chain: Chain): string {
  const tokens = getAddressBookTokensConfig(chain);
  return normalizeAddressOrThrow(tokens.WNATIVE.address);
}
export function getChainWNativeTokenDecimals(chain: Chain): number {
  const tokens = getAddressBookTokensConfig(chain);
  return tokens.WNATIVE.decimals;
}
export function getChainWNativeTokenSymbol(chain: Chain): string {
  const tokens = getAddressBookTokensConfig(chain);
  return tokens.WNATIVE.symbol;
}
export function getChainWNativeTokenOracleId(chain: Chain): string {
  const tokens = getAddressBookTokensConfig(chain);
  return tokens.WNATIVE.symbol;
}

export function getAddressBookTokenDecimals(chain: Chain, token: string): number {
  const tokens = getAddressBookTokensConfig(chain);
  if (!tokens[token]) {
    throw new Error(`Token ${token} not found in addressbook for chain ${chain}`);
  }
  return tokens[token].decimals;
}

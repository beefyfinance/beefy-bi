import { Chain } from "../types/chain";
import { normalizeAddress } from "./ethers";
import * as addressbook from "blockchain-addressbook";

function getAddressBookTokensConfig(chain: Chain) {
  const addrBookChain = chain === "harmony" ? "one" : chain === "syscoin" ? "sys" : chain;
  return addressbook.addressBook[addrBookChain].tokens;
}

export function getChainWNativeTokenAddress(chain: Chain): string {
  const tokens = getAddressBookTokensConfig(chain);
  return normalizeAddress(tokens.WNATIVE.address);
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

import * as addressbook from "blockchain-addressbook";
import { Chain } from "../types/chain";
import { normalizeAddress } from "./ethers";

function getAddressBookTokensConfig(chain: Chain) {
  if (chain === "ethereum") {
    return {
      WNATIVE: {
        decimals: 18,
        symbol: "WETH",
        address: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
      },
    };
  }
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

import * as addressbook from "blockchain-addressbook";
import { Chain } from "../types/chain";
import { normalizeAddressOrThrow } from "./ethers";

export function getBridgedVaultTargetChains(): Chain[] {
  return ["optimism", "base"/*, "sonic"*/];
}

export function getBridgedVaultOriginChains(): Chain[] {
  return ["ethereum"];
}

function getAddressBookTokensConfig(chain: Chain) {
  const addrBookChain = chain === "harmony" ? "one" : chain;

  if (addrBookChain in addressbook.addressBook) {
    // @ts-ignore
    return addressbook.addressBook[addrBookChain].tokens;
  } else if (chain === "sonic") {
    return {
      WNATIVE: { address: "0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38", symbol: "wS" },
    };
  }

  throw new Error(`Unknown chain ${chain}`);
}

export function getChainNetworkId(chain: Chain): number {
  const addrBookChain = chain === "harmony" ? "one" : chain;
  if (addrBookChain in addressbook.ChainId) {
    // @ts-ignore
    return addressbook.ChainId[addrBookChain];
  } else if (chain === "sonic") {
    return 146;
  }
  throw new Error(`Unknown chain ${chain}`);
}

export function getChainWNativeTokenAddress(chain: Chain): string {
  const tokens = getAddressBookTokensConfig(chain);
  return normalizeAddressOrThrow(tokens.WNATIVE.address);
}

export function getChainWNativeTokenSymbol(chain: Chain): string {
  const tokens = getAddressBookTokensConfig(chain);
  return tokens.WNATIVE.symbol;
}

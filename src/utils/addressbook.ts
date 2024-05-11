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

  if (addrBookChain in addressbook.addressBook) {
    // @ts-ignore
    return addressbook.addressBook[addrBookChain].tokens;
  } else if (chain === "mode") {
    return {
      WNATIVE: { address: "0x4200000000000000000000000000000000000006" },
    };
  } else if (chain === "scroll") {
    return {
      WNATIVE: { address: "0x5300000000000000000000000000000000000004" },
    };
  } else if (chain === "rollux") {
    return {
      WNATIVE: { address: "0x4200000000000000000000000000000000000006" },
    };
  }

  throw new Error(`Unknown chain ${chain}`);
}

export function getChainNetworkId(chain: Chain): number {
  const addrBookChain = chain === "harmony" ? "one" : chain;
  if (addrBookChain in addressbook.ChainId) {
    // @ts-ignore
    return addressbook.ChainId[addrBookChain];
  } else if (chain === "scroll") {
    return 534352;
  } else if (chain === "rollux") {
    return 570;
  } else if (chain === "mode") {
    return 34443;
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

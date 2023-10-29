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
  } else if (chain === "linea") {
    return {
      WNATIVE: { address: "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f" },
    };
  } else if (chain === "scroll") {
    return {
      WNATIVE: { address: "0x5300000000000000000000000000000000000004" },
    };
  }

  throw new Error(`Unknown chain ${chain}`);
}

export function getChainNetworkId(chain: Chain): number {
  const addrBookChain = chain === "harmony" ? "one" : chain;
  if (addrBookChain in addressbook.ChainId) {
    // @ts-ignore
    return addressbook.ChainId[addrBookChain];
  } else if (chain === "linea") {
    return 59144;
  } else if (chain === "scroll") {
    return 534352;
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

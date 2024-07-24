import { Chain } from "../../types/chain";
import {
  RPC_API_KEY_ALCHEMY_ARBITRUM,
  RPC_API_KEY_ALCHEMY_OPTIMISM,
  RPC_API_KEY_ANKR,
  RPC_API_KEY_AURORA,
  RPC_API_KEY_FIGMENT,
  RPC_API_KEY_GETBLOCK,
  RPC_API_KEY_INFURA,
  RPC_API_KEY_LLAMARPC,
  RPC_API_KEY_METIS_OWNER,
  RPC_API_KEY_NODEREAL,
  RPC_API_KEY_NODEREAL_2,
  RPC_API_URL_CHAINSTACK_CRONOS,
  RPC_API_URL_FUSE_BEEFY,
  RPC_API_URL_KAVA_BEEFY,
  RPC_API_URL_QUIKNODE_ARBITRUM,
  RPC_API_URL_QUIKNODE_AVAX,
  RPC_API_URL_QUIKNODE_BASE,
  RPC_API_URL_QUIKNODE_FANTOM,
  RPC_API_URL_QUIKNODE_GNOSIS,
  RPC_API_URL_QUIKNODE_MANTLE,
  RPC_API_URL_QUIKNODE_OPTIMISM,
  RPC_API_URL_QUIKNODE_POLYGON,
  RPC_API_URL_QUIKNODE_ZKEVM,
  RPC_API_URL_QUIKNODE_ZKSYNC,
} from "../config";
import { ProgrammerError } from "../programmer-error";

export function removeSecretsFromRpcUrl(chain: Chain, secretRpcUrl: string): string {
  let publicRpcUrl = "";
  let pathParts: string[] = [];

  if (secretRpcUrl.startsWith("http")) {
    const urlObj = new URL(secretRpcUrl);

    // clean user and password from domain
    publicRpcUrl = urlObj.protocol + "//" + urlObj.hostname + (urlObj.port.length ? ":" + urlObj.port : "");

    // cleanup url params
    let cleanedPath = urlObj.pathname.trim().replace(/^\//, "").replace(/\/$/, "");
    pathParts = cleanedPath.split("/").filter((part) => part.length);
  }

  if (secretRpcUrl.includes("mainnet.aurora.dev") && pathParts.length > 0) {
    publicRpcUrl += "/<RPC_API_KEY_AURORA>";
  } else if (secretRpcUrl.includes("ankr")) {
    const chain = pathParts[0];
    if (pathParts.length === 2) {
      publicRpcUrl += "/" + chain + "/<RPC_API_KEY_ANKR>";
    } else {
      publicRpcUrl += "/" + chain;
    }
  } else if (secretRpcUrl.includes("andromeda.metis.io") && secretRpcUrl.includes("owner=")) {
    publicRpcUrl += "/?owner=<RPC_API_KEY_METIS_OWNER>";
  } else if (secretRpcUrl.includes("alchemy.com") && pathParts.length === 2 && pathParts[0] === "v2") {
    if (secretRpcUrl.includes("opt-mainnet")) {
      publicRpcUrl += "/v2/<RPC_API_KEY_ALCHEMY_OPTIMISM>";
    } else if (secretRpcUrl.includes("arb-mainnet")) {
      publicRpcUrl += "/v2/<RPC_API_KEY_ALCHEMY_ARBITRUM>";
    }
  } else if (secretRpcUrl.includes("nodereal.io") && pathParts.length === 2 && pathParts[0] === "v1") {
    publicRpcUrl += "/v1/<RPC_API_KEY_NODEREAL>";
  } else if (secretRpcUrl.includes("figment.io") && pathParts.length === 2 && pathParts[0] === "apikey") {
    publicRpcUrl += "/apikey/<RPC_API_KEY_FIGMENT>";
  } else if (secretRpcUrl.includes(".getblock.io")) {
    publicRpcUrl += "/<RPC_API_KEY_GETBLOCK>/mainnet";
  } else if (secretRpcUrl.includes(".infura.io/v3")) {
    publicRpcUrl += "/v3/<RPC_API_KEY_INFURA>";
  } else if (secretRpcUrl.includes(".quiknode.pro")) {
    if (secretRpcUrl.includes("arbitrum-mainnet.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_ARBITRUM>";
    } else if (secretRpcUrl.includes("avalanche-mainnet.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_AVAX>";
    } else if (secretRpcUrl.includes("base-mainnet.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_BASE>";
    } else if (secretRpcUrl.includes("xdai.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_GNOSIS>";
    } else if (secretRpcUrl.includes("mantle-mainnet.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_MANTLE>";
    } else if (secretRpcUrl.includes("optimism.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_OPTIMISM>";
    } else if (secretRpcUrl.includes("matic.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_POLYGON>";
    } else if (secretRpcUrl.includes("zksync-mainnet.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_ZKSYNC>";
    } else if (secretRpcUrl.includes("fantom.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_FANTOM>";
    } else if (secretRpcUrl.includes("zkevm-mainnet.quiknode.pro")) {
      publicRpcUrl = "<RPC_API_URL_QUIKNODE_ZKEVM>";
    }
  } else if (secretRpcUrl.includes("llamarpc.com")) {
    publicRpcUrl += "/rpc/<RPC_API_KEY_LLAMARPC>";
  } else if (secretRpcUrl.includes("kava") && secretRpcUrl.includes("beefy")) {
    publicRpcUrl = "<RPC_API_URL_KAVA_BEEFY>";
  } else if (secretRpcUrl.includes("fuse") && secretRpcUrl.includes("beefy")) {
    publicRpcUrl = "<RPC_API_URL_FUSE_BEEFY>";
  } else if (secretRpcUrl.includes("p2pify.com")) {
    if (chain === "cronos") {
      publicRpcUrl = "<RPC_API_URL_CHAINSTACK_CRONOS>";
    } else {
      throw new ProgrammerError({ msg: `Chain config not defined for chainstack`, data: { chain } });
    }
  } else {
    if (pathParts.length > 0) {
      publicRpcUrl += "/" + pathParts.join("/");
    }
  }
  return publicRpcUrl;
}

export function addSecretsToRpcUrl(publicRpcUrl: string): string {
  let url = publicRpcUrl;

  function replaceFromConfigOrThrow(url: string, str: string, config: string | null) {
    if (publicRpcUrl.includes("<" + str + ">")) {
      if (config) {
        return publicRpcUrl.replace("<" + str + ">", config);
      } else {
        throw new ProgrammerError("Missing config " + str);
      }
    }
    return url;
  }

  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_ANKR", RPC_API_KEY_ANKR);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_AURORA", RPC_API_KEY_AURORA);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_METIS_OWNER", RPC_API_KEY_METIS_OWNER);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_NODEREAL_2", RPC_API_KEY_NODEREAL_2);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_NODEREAL", RPC_API_KEY_NODEREAL);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_FIGMENT", RPC_API_KEY_FIGMENT);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_GETBLOCK", RPC_API_KEY_GETBLOCK);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_ALCHEMY_OPTIMISM", RPC_API_KEY_ALCHEMY_OPTIMISM);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_ALCHEMY_ARBITRUM", RPC_API_KEY_ALCHEMY_ARBITRUM);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_INFURA", RPC_API_KEY_INFURA);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_CHAINSTACK_CRONOS", RPC_API_URL_CHAINSTACK_CRONOS);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_ARBITRUM", RPC_API_URL_QUIKNODE_ARBITRUM);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_AVAX", RPC_API_URL_QUIKNODE_AVAX);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_BASE", RPC_API_URL_QUIKNODE_BASE);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_FANTOM", RPC_API_URL_QUIKNODE_FANTOM);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_GNOSIS", RPC_API_URL_QUIKNODE_GNOSIS);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_MANTLE", RPC_API_URL_QUIKNODE_MANTLE);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_OPTIMISM", RPC_API_URL_QUIKNODE_OPTIMISM);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_POLYGON", RPC_API_URL_QUIKNODE_POLYGON);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_ZKEVM", RPC_API_URL_QUIKNODE_ZKEVM);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_QUIKNODE_ZKSYNC", RPC_API_URL_QUIKNODE_ZKSYNC);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_KAVA_BEEFY", RPC_API_URL_KAVA_BEEFY);
  url = replaceFromConfigOrThrow(url, "RPC_API_URL_FUSE_BEEFY", RPC_API_URL_FUSE_BEEFY);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_LLAMARPC", RPC_API_KEY_LLAMARPC);

  return url;
}

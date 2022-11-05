import { RPC_API_KEY_ALCHEMY, RPC_API_KEY_ANKR, RPC_API_KEY_AURORA, RPC_API_KEY_METIS_OWNER } from "../config";
import { ProgrammerError } from "../programmer-error";

export function removeSecretsFromRpcUrl(secretRpcUrl: string): string {
  const urlObj = new URL(secretRpcUrl);

  // clean user and password from domain
  let publicRpcUrl = urlObj.protocol + "//" + urlObj.hostname + (urlObj.port.length ? ":" + urlObj.port : "");

  // cleanup url params
  let cleanedPath = urlObj.pathname.trim().replace(/^\//, "").replace(/\/$/, "");
  const pathParts = cleanedPath.split("/").filter((part) => part.length);

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
    publicRpcUrl += "/v2/<RPC_API_KEY_ALCHEMY>";
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

  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_ALCHEMY", RPC_API_KEY_ALCHEMY);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_ANKR", RPC_API_KEY_ANKR);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_AURORA", RPC_API_KEY_AURORA);
  url = replaceFromConfigOrThrow(url, "RPC_API_KEY_METIS_OWNER", RPC_API_KEY_METIS_OWNER);

  return url;
}

export function removeSecretsFromRpcUrl(secretRpcUrl: string): string {
  const urlObj = new URL(secretRpcUrl);
  let publicRpcUrl = urlObj.protocol + "//" + urlObj.hostname + (urlObj.port.length ? ":" + urlObj.port : "");
  if (secretRpcUrl.includes("ankr")) {
    publicRpcUrl += "/" + urlObj.pathname.split("/")[1];
  }
  return publicRpcUrl;
}

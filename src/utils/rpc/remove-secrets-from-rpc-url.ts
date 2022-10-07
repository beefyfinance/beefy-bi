export function removeSecretsFromRpcUrl(secretRpcUrl: string): string {
  const urlObj = new URL(secretRpcUrl);
  let publicRpcUrl = urlObj.protocol + "//" + urlObj.hostname;
  if (secretRpcUrl.includes("ankr")) {
    publicRpcUrl += urlObj.pathname.split("/")[0];
  }
  return publicRpcUrl;
}

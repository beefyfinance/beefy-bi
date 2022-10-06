export function removeSecretsFromRpcUrl(secretRpcUrl: string): string {
  const urlObj = new URL(secretRpcUrl);
  const publicRpcUrl = urlObj.protocol + "//" + urlObj.hostname;
  return publicRpcUrl;
}

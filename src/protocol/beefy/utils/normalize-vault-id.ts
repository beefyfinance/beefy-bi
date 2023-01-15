// the only unique field is the address as beefy devs change the id
// to add "-eol" at the end when the vault is retired
export function normalizeVaultId(vaultId: string) {
  return vaultId.replace(/-eol$/, "").replace(/-pause$/, "");
}

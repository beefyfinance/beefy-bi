import { DbBeefyProduct, DbBeefyVaultProduct } from "../../common/loader/product";

export function isBeefyGovVault(o: DbBeefyProduct): o is DbBeefyVaultProduct {
  return o.productData.type === "beefy:vault" && o.productData.vault.is_gov_vault;
}
export function isBeefyStandardVault(o: DbBeefyProduct): o is DbBeefyVaultProduct {
  return o.productData.type === "beefy:vault" && !o.productData.vault.is_gov_vault;
}
export function isBeefyBoost(o: DbBeefyProduct): o is DbBeefyVaultProduct {
  return o.productData.type === "beefy:boost";
}

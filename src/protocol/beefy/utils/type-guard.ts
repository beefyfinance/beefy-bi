import { DbBeefyBoostProduct, DbBeefyProduct, DbBeefyVaultProduct } from "../../common/loader/product";
import { ImportQuery } from "../../common/types/import-query";

export function isBeefyGovVault(o: DbBeefyProduct): o is DbBeefyVaultProduct {
  return o.productData.type === "beefy:vault" && o.productData.vault.is_gov_vault;
}
export function isBeefyStandardVault(o: DbBeefyProduct): o is DbBeefyVaultProduct {
  return o.productData.type === "beefy:vault" && !o.productData.vault.is_gov_vault;
}
export function isBeefyBoost(o: DbBeefyProduct): o is DbBeefyBoostProduct {
  return o.productData.type === "beefy:boost";
}

export function isBeefyGovVaultProductImportQuery(o: ImportQuery<DbBeefyProduct>): o is ImportQuery<DbBeefyVaultProduct> {
  return isBeefyGovVault(o.target);
}
export function isBeefyStandardVaultProductImportQuery(o: ImportQuery<DbBeefyProduct>): o is ImportQuery<DbBeefyVaultProduct> {
  return isBeefyStandardVault(o.target);
}
export function isBeefyBoostProductImportQuery(o: ImportQuery<DbBeefyProduct>): o is ImportQuery<DbBeefyBoostProduct> {
  return isBeefyBoost(o.target);
}

import { DbBeefyBoostProduct, DbBeefyProduct, DbBeefyVaultProduct } from "../../common/loader/product";
import { ProductImportQuery } from "../../common/types/product-query";

export function isBeefyGovVault(o: DbBeefyProduct): o is DbBeefyVaultProduct {
  return o.productData.type === "beefy:vault" && o.productData.vault.is_gov_vault;
}
export function isBeefyStandardVault(o: DbBeefyProduct): o is DbBeefyVaultProduct {
  return o.productData.type === "beefy:vault" && !o.productData.vault.is_gov_vault;
}
export function isBeefyBoost(o: DbBeefyProduct): o is DbBeefyBoostProduct {
  return o.productData.type === "beefy:boost";
}

export function isBeefyGovVaultProductImportQuery(o: ProductImportQuery<DbBeefyProduct>): o is ProductImportQuery<DbBeefyVaultProduct> {
  return isBeefyGovVault(o.product);
}
export function isBeefyStandardVaultProductImportQuery(o: ProductImportQuery<DbBeefyProduct>): o is ProductImportQuery<DbBeefyVaultProduct> {
  return isBeefyStandardVault(o.product);
}
export function isBeefyBoostProductImportQuery(o: ProductImportQuery<DbBeefyProduct>): o is ProductImportQuery<DbBeefyBoostProduct> {
  return isBeefyBoost(o.product);
}

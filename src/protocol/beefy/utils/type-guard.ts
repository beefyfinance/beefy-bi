import { DbBeefyBoostProduct, DbBeefyProduct, DbBeefyStdVaultProduct } from "../../common/loader/product";
import { ImportQuery } from "../../common/types/import-query";

export function isBeefyGovVault(o: DbBeefyProduct): o is DbBeefyStdVaultProduct {
  return o.productData.type === "beefy:gov-vault";
}
export function isBeefyStandardVault(o: DbBeefyProduct): o is DbBeefyStdVaultProduct {
  return o.productData.type === "beefy:vault";
}
export function isBeefyBoost(o: DbBeefyProduct): o is DbBeefyBoostProduct {
  return o.productData.type === "beefy:boost";
}

export function isBeefyGovVaultProductImportQuery(o: ImportQuery<DbBeefyProduct>): o is ImportQuery<DbBeefyStdVaultProduct> {
  return isBeefyGovVault(o.target);
}
export function isBeefyStandardVaultProductImportQuery(o: ImportQuery<DbBeefyProduct>): o is ImportQuery<DbBeefyStdVaultProduct> {
  return isBeefyStandardVault(o.target);
}
export function isBeefyBoostProductImportQuery(o: ImportQuery<DbBeefyProduct>): o is ImportQuery<DbBeefyBoostProduct> {
  return isBeefyBoost(o.target);
}

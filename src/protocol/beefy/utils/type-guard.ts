import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbBeefyStdVaultProduct } from "../../common/loader/product";
import { ImportQuery } from "../../common/types/import-query";

export function isBeefyGovVault(o: DbBeefyProduct): o is DbBeefyGovVaultProduct {
  return o.productData.type === "beefy:gov-vault";
}
export function isBeefyStandardVault(o: DbBeefyProduct): o is DbBeefyStdVaultProduct {
  return o.productData.type === "beefy:vault";
}
export function isBeefyBoost(o: DbBeefyProduct): o is DbBeefyBoostProduct {
  return o.productData.type === "beefy:boost";
}

export function isBeefyGovVaultProductImportQuery(o: ImportQuery<DbBeefyProduct, number>): o is ImportQuery<DbBeefyGovVaultProduct, number> {
  return isBeefyGovVault(o.target);
}
export function isBeefyStandardVaultProductImportQuery(o: ImportQuery<DbBeefyProduct, number>): o is ImportQuery<DbBeefyStdVaultProduct, number> {
  return isBeefyStandardVault(o.target);
}
export function isBeefyBoostProductImportQuery(o: ImportQuery<DbBeefyProduct, number>): o is ImportQuery<DbBeefyBoostProduct, number> {
  return isBeefyBoost(o.target);
}

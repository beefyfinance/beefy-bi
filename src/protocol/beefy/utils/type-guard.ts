import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbBeefyStdVaultProduct } from "../../common/loader/product";
import { ImportRangeQuery } from "../../common/types/import-query";

export function isBeefyGovVault(o: DbBeefyProduct): o is DbBeefyGovVaultProduct {
  return o.productData.type === "beefy:gov-vault";
}
export function isBeefyStandardVault(o: DbBeefyProduct): o is DbBeefyStdVaultProduct {
  return o.productData.type === "beefy:vault";
}
export function isBeefyBoost(o: DbBeefyProduct): o is DbBeefyBoostProduct {
  return o.productData.type === "beefy:boost";
}

export function isBeefyGovVaultProductImportQuery(
  o: ImportRangeQuery<DbBeefyProduct, number>,
): o is ImportRangeQuery<DbBeefyGovVaultProduct, number> {
  return isBeefyGovVault(o.target);
}
export function isBeefyStandardVaultProductImportQuery(
  o: ImportRangeQuery<DbBeefyProduct, number>,
): o is ImportRangeQuery<DbBeefyStdVaultProduct, number> {
  return isBeefyStandardVault(o.target);
}
export function isBeefyBoostProductImportQuery(o: ImportRangeQuery<DbBeefyProduct, number>): o is ImportRangeQuery<DbBeefyBoostProduct, number> {
  return isBeefyBoost(o.target);
}

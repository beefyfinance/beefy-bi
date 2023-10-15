import {
  DbBeefyBoostProduct,
  DbBeefyBridgedVaultProduct,
  DbBeefyGovVaultProduct,
  DbBeefyProduct,
  DbBeefyStdVaultProduct,
} from "../../common/loader/product";
import { ImportRangeQuery } from "../../common/types/import-query";

export function isBeefyGovVault(o: DbBeefyProduct): o is DbBeefyGovVaultProduct {
  return o.productData.type === "beefy:gov-vault";
}
export function isBeefyStandardVault(o: DbBeefyProduct): o is DbBeefyStdVaultProduct {
  return o.productData.type === "beefy:vault";
}
export function isBeefyBridgedVault(o: DbBeefyProduct): o is DbBeefyBridgedVaultProduct {
  return o.productData.type === "beefy:bridged-vault";
}
export function isBeefyBoost(o: DbBeefyProduct): o is DbBeefyBoostProduct {
  return o.productData.type === "beefy:boost";
}

export function isBeefyStandardVaultProductImportQuery(
  o: ImportRangeQuery<DbBeefyProduct, number>,
): o is ImportRangeQuery<DbBeefyStdVaultProduct, number> {
  return isBeefyStandardVault(o.target);
}
export function isBeefyGovVaultOrBoostProductImportQuery(
  o: ImportRangeQuery<DbBeefyProduct, number>,
): o is ImportRangeQuery<DbBeefyBoostProduct | DbBeefyGovVaultProduct, number> {
  return isBeefyGovVault(o.target) || isBeefyBoost(o.target);
}

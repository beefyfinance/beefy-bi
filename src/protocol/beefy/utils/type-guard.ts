import * as Rx from "rxjs";
import { OverwriteKeyType } from "../../../types/ts";
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
export function isBeefyGovVaultOrBoostProductImportQuery(
  o: ImportRangeQuery<DbBeefyProduct, number>,
): o is ImportRangeQuery<DbBeefyBoostProduct | DbBeefyGovVaultProduct, number> {
  return isBeefyGovVault(o.target) || isBeefyBoost(o.target);
}

export function isBeefyBoostProductImportQuery(o: ImportRangeQuery<DbBeefyProduct, number>): o is ImportRangeQuery<DbBeefyBoostProduct, number> {
  return isBeefyBoost(o.target);
}

export function filterStandardVault$<Input, TKey extends keyof Input>(
  key: TKey,
): Rx.OperatorFunction<Input, OverwriteKeyType<Input, { [k in TKey]: DbBeefyStdVaultProduct }>> {
  // @ts-ignore
  return Rx.filter((o: Input) => {
    const product = o[key] as DbBeefyProduct;
    return isBeefyStandardVault(product);
  });
}
export function filterGovVault$<Input, TKey extends keyof Input>(
  key: TKey,
): Rx.OperatorFunction<Input, OverwriteKeyType<Input, { [k in TKey]: DbBeefyGovVaultProduct }>> {
  // @ts-ignore
  return Rx.filter((o: Input) => {
    const product = o[key] as DbBeefyProduct;
    return isBeefyGovVault(product);
  });
}
export function filterBoost$<Input, TKey extends keyof Input>(
  key: TKey,
): Rx.OperatorFunction<Input, OverwriteKeyType<Input, { [k in TKey]: DbBeefyBoostProduct }>> {
  // @ts-ignore
  return Rx.filter((o: Input) => {
    const product = o[key] as DbBeefyProduct;
    return isBeefyBoost(product);
  });
}

import { samplingPeriodMs } from "../../../types/sampling";
import { rootLogger } from "../../../utils/logger";
import { DbProduct } from "../loader/product";
import { ImportBehavior } from "../types/import-context";

const logger = rootLogger.child({ module: "beefy-util", component: "eol" });

export function isProductDashboardEOL(product: DbProduct): boolean {
  // special case where dashboardEol is still not set
  if (product.productData.dashboardEol === undefined) {
    return false;
  }
  return product.productData.dashboardEol;
}

export function computeIsDashboardEOL(behavior: ImportBehavior, eol: boolean, eolDate: Date | null) {
  let isDashboardEol = false;
  // special case where we can't find when the product was eol'ed
  if (eol && eolDate === null) {
    logger.warn({ msg: "eol_date is null" });
  } else if (eol && eolDate !== null) {
    isDashboardEol = eolDate.getTime() < Date.now() - samplingPeriodMs[behavior.productIsDashboardEolAfter];
  }
  return isDashboardEol;
}

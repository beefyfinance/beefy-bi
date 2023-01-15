import { DbProduct } from "../loader/product";

export function isProductDashboardEOL(product: DbProduct): boolean {
  // special case where dashboardEol is still not set
  if (product.productData.dashboardEol === undefined) {
    return false;
  }
  return product.productData.dashboardEol;
}
